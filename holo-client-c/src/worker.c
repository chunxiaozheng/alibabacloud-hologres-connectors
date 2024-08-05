#include "worker.h"
#include "logger_private.h"
#include <libpq-fe.h>
#include "utils.h"
#include "action.h"
#include <stdlib.h>
#include "table_schema_private.h"
#include "batch.h"
#include "sql_builder.h"
#include "exception.h"
#include "inttypes.h"

#define MAX_PARAM_NUM 32767 // max number of parameters in PG's prepared statement

typedef ActionStatus (*BatchHandler)(ConnectionHolder*, Batch*, dlist_node**, int, char**);

ActionStatus get_holo_version(ConnectionHolder*);
ActionStatus get_table_schema(ConnectionHolder*, HoloTableSchema*, HoloTableName, char**);
ActionStatus handle_mutations(ConnectionHolder*, dlist_head*, char**);
void add_mutation_item_to_batch_list(MutationItem*, dlist_head*);
ActionStatus handle_batch_list(dlist_head*, ConnectionHolder*, char**);
ActionStatus handle_batch(Batch*, int, BatchHandler, ConnectionHolder*, char**);
ActionStatus exec_batch(ConnectionHolder*, Batch*, dlist_node**, int, char**);
ActionStatus exec_batch_one_by_one(ConnectionHolder*, Batch*, dlist_node**, int, char**);
ActionStatus delete_batch(ConnectionHolder*, Batch*, dlist_node**, int);
ActionStatus handle_sql(ConnectionHolder* connHolder, SqlFunction sqlFunction, void* arg, void** retAddr);
ActionStatus handle_gets(ConnectionHolder*, HoloTableSchema*, dlist_head*, int);

extern int unnest_convert_array_to_text(char**, char**, int*, int);
extern void unnest_convert_array_to_postgres_binary(char*, void*, int, int, int);
extern void convert_text_array_to_postgres_binary(char*, char**, int, int);

Worker* holo_client_new_worker(HoloConfig config, int index, bool isFixedFe) {
    Worker* worker = MALLOC(1, Worker);
    worker->connHolder = holo_client_new_connection_holder(config, isFixedFe);
    worker->action = NULL;
    worker->config = config;
    if (isFixedFe) {
        worker->config.connInfo = generate_fixed_fe_conn_info(config.connInfo);
    } else {
        worker->config.connInfo = deep_copy_string(config.connInfo);
    }
    worker->index = index;
    worker->status = 0;
    worker->thread = MALLOC(1, pthread_t);
    worker->mutex = MALLOC(1, pthread_mutex_t);
    worker->cond = MALLOC(1, pthread_cond_t);
    pthread_mutex_init(worker->mutex, NULL);
    pthread_cond_init(worker->cond, NULL);
    worker->metrics = holo_client_new_metrics_in_worker();
    worker->connHolder->metrics = worker->metrics;
    worker->lastUpdateTime = current_time_ms();
    worker->idleMutex = NULL;
    worker->idleCond = NULL;
    worker->map = holo_client_new_lp_map(config.readBatchSize);
    worker->connHolder->map =  worker->map;
    return worker;
}

void* worker_run(void* workerPtr) {
    Worker* worker = workerPtr;
    pthread_mutex_lock(worker->mutex);
    while(worker->status == 1) {
        if(worker->action != NULL) {
            metrics_histogram_update(worker->metrics->idleTime, current_time_ms() - worker->lastUpdateTime);
            worker->lastUpdateTime = current_time_ms();
            ActionStatus rc;
            switch(worker->action->type){
            case 0:
                rc = connection_holder_do_action(worker->connHolder, worker->action, handle_meta_action);
                break;
            case 1:
                rc = connection_holder_do_action(worker->connHolder, worker->action, handle_mutation_action);
                break;
            case 2:
                rc = connection_holder_do_action(worker->connHolder, worker->action, handle_sql_action);
                break;
            case 3:
                rc = connection_holder_do_action(worker->connHolder, worker->action, handle_get_action);
                break;
            default:
                rc = FAILURE_NOT_NEED_RETRY;
            }
            if (rc != SUCCESS) worker_abort_action(worker);
            metrics_histogram_update(worker->metrics->handleActionTime, current_time_ms() - worker->lastUpdateTime);
            worker->lastUpdateTime = current_time_ms();
            worker->action = NULL;
        }
        pthread_cond_signal(worker->idleCond);
        struct timespec out_time = get_out_time(worker->config.connectionMaxIdleMs);
        if (pthread_cond_timedwait(worker->cond, worker->mutex, &out_time) != 0){  //空闲超时，关闭连接
            LOG_INFO("Worker %d idle time out.", worker->index);
            connection_holder_close_conn(worker->connHolder);
            pthread_cond_signal(worker->idleCond);
            pthread_cond_wait(worker->cond, worker->mutex);
        };
    }
    worker->status = 3; 
    connection_holder_close_conn(worker->connHolder);
    pthread_mutex_unlock(worker->mutex);
    return NULL;
}

int holo_client_start_worker(Worker* worker) {
    int rc;
    worker->status = 1;
    metrics_meter_reset(worker->metrics->qps);
    metrics_meter_reset(worker->metrics->rps);
    worker->lastUpdateTime = current_time_ms();
    rc = pthread_create(worker->thread, NULL, worker_run, worker);
    if (rc != 0) {
        worker->status = 4;
        LOG_ERROR("Worker %d started failed with error code %d.", worker->index, rc);
    }
    LOG_DEBUG("Worker %d started.", worker->index);
    return rc;
}

int holo_client_stop_worker(Worker* worker) {
    int rc;
    pthread_mutex_lock(worker->mutex);
    worker->status = 2;
    pthread_cond_signal(worker->cond);
    pthread_mutex_unlock(worker->mutex);
    rc = pthread_join(*worker->thread, NULL);
    LOG_DEBUG("Worker %d stopped.", worker->index);
    worker->status = 3;
    return rc;
}

void holo_client_close_worker(Worker* worker) {
    //释放资源
    pthread_mutex_destroy(worker->mutex);
    pthread_cond_destroy(worker->cond);
    FREE(worker->connHolder->holoVersion);
    FREE(worker->connHolder->connInfo);
    FREE(worker->connHolder);
    FREE(worker->config.connInfo);
    FREE(worker->thread);
    FREE(worker->mutex);
    FREE(worker->cond);
    holo_client_destroy_metrics_in_worker(worker->metrics);
    holo_client_destroy_lp_map(worker->map);
    FREE(worker);
    worker = NULL;
}

bool holo_client_try_submit_action_to_worker(Worker* worker, Action* action) {
    bool success = false;
    if(pthread_mutex_trylock(worker->mutex) == 0) {
        if(worker->status == 1 && worker->action == NULL) {
            worker->action = action;
            success = true;
            pthread_cond_signal(worker->cond);
        }
        pthread_mutex_unlock(worker->mutex);
    }
    return success;
}

ActionStatus handle_meta_action(ConnectionHolder* connHolder, Action* action) {
    ActionStatus rc = get_holo_version(connHolder);
    // 若获取holo版本失败，仍然尝试获取table schema
    HoloTableSchema* schema = holo_client_new_tableschema();
    rc = get_table_schema(connHolder, schema, ((MetaAction*)action)->meta->tableName, &(((MetaAction*)action)->meta->future->errMsg));
    if (rc != SUCCESS) return rc;
    complete_future(((MetaAction*)action)->meta->future, schema);
    holo_client_destroy_meta_action((MetaAction*)action);
    return rc;
}

ActionStatus handle_mutation_action(ConnectionHolder* connHolder, Action* action) {
    ActionStatus rc = handle_mutations(connHolder, &((MutationAction*)action)->requests, &(((MutationAction*)action)->future->errMsg));
    if (rc != SUCCESS) return rc;
    complete_future(((MutationAction*)action)->future, action);
    return rc;
}

ActionStatus handle_sql_action(ConnectionHolder* connHolder, Action* action) {
    void* retVal = NULL;
    ActionStatus rc = handle_sql(connHolder, ((SqlAction*)action)->sql->sqlFunction, ((SqlAction*)action)->sql->arg, &retVal);
    if (rc != SUCCESS) return rc;
    complete_future(((SqlAction*)action)->sql->future, retVal);
    holo_client_destroy_sql_action((SqlAction*)action);
    return rc;
}

ActionStatus handle_get_action(ConnectionHolder* connHolder, Action* action) {
    // LOG_DEBUG("num get requests: %d", ((GetAction*)action)->numRequests);
    ActionStatus rc = handle_gets(connHolder, ((GetAction*)action)->schema, &((GetAction*)action)->requests, ((GetAction*)action)->numRequests);
    if (rc != SUCCESS) return rc;
    holo_client_destroy_get_action((GetAction*)action);
    return rc;
}

void worker_abort_action(Worker* worker){
    switch(worker->action->type){
        case 0:
            complete_future(((MetaAction*)worker->action)->meta->future, NULL);
            holo_client_destroy_meta_action((MetaAction*)worker->action);
            break;
        case 1:
            complete_future(((MutationAction*)worker->action)->future, NULL);
            break;
        case 2:
            complete_future(((SqlAction*)worker->action)->sql->future, NULL);
            holo_client_destroy_sql_action((SqlAction*)worker->action);
            break;
        case 3:
            abort_get_action((GetAction*)worker->action);
            holo_client_destroy_get_action((GetAction*)worker->action);
            break;
        default:
            LOG_ERROR("Worker abort action falied. Invalid action type: %d", worker->action->type);
            break;
        }
}

ActionStatus get_holo_version(ConnectionHolder* connHolder) {
    PGresult* res = NULL;
    const char* findHgVersion = "select hg_version()";
    const char* findHoloVersion = "select version()";

    res = connection_holder_exec_params_with_retry(connHolder, findHgVersion, 0, NULL, NULL, NULL, NULL, 0, NULL);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0){
        LOG_WARN("Get Hg Version failed.");
        if (res != NULL) {
            PQclear(res);
        }
        return FAILURE_NOT_NEED_RETRY;
    } else {
        char* hgVersionStr = deep_copy_string(PQgetvalue(res, 0, 0));
        int cnt = sscanf(hgVersionStr, "Hologres %d.%d.%d", &connHolder->holoVersion->majorVersion, &connHolder->holoVersion->minorVersion, &connHolder->holoVersion->fixVersion);
        FREE(hgVersionStr);
        if (res != NULL) {
            PQclear(res);
        }
        if (cnt > 0) {
            LOG_DEBUG("Get Hg Version: %d.%d.%d", connHolder->holoVersion->majorVersion, connHolder->holoVersion->minorVersion, connHolder->holoVersion->fixVersion);
            return SUCCESS;
        } else {
            LOG_WARN("Get Hg Version failed.");
            return FAILURE_NOT_NEED_RETRY;
        }
    }

    res = connection_holder_exec_params_with_retry(connHolder, findHoloVersion, 0, NULL, NULL, NULL, NULL, 0, NULL);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0){
        LOG_WARN("Get Holo Version failed.");
        if (res != NULL) {
            PQclear(res);
        }
        return FAILURE_NOT_NEED_RETRY;
    } else {
        char* holoVersionStr = deep_copy_string(PQgetvalue(res, 0, 0));
        int cnt = sscanf(holoVersionStr, "release-%d.%d.%d", &connHolder->holoVersion->majorVersion, &connHolder->holoVersion->minorVersion, &connHolder->holoVersion->fixVersion);
        FREE(holoVersionStr);
        if (res != NULL) {
            PQclear(res);
        }
        if (cnt > 0) {
            LOG_DEBUG("Get Holo Version: %d.%d.%d", connHolder->holoVersion->majorVersion, connHolder->holoVersion->minorVersion, connHolder->holoVersion->fixVersion);
            return SUCCESS;
        } else {
            LOG_WARN("Get Holo Version failed.");
            return FAILURE_NOT_NEED_RETRY;
        }
    }
}

ActionStatus get_table_schema(ConnectionHolder* connHolder, HoloTableSchema* schema, HoloTableName tableName, char** errMsgAddr) {
    HoloColumn* columns = NULL;
    const char* findTableOidSql = "SELECT property_value FROM hologres.hg_table_properties WHERE table_namespace = $1 AND table_name = $2 AND property_key = 'table_id'";
    const char* findColumnsSql = "WITH c AS (SELECT column_name, ordinal_position, is_nullable, column_default FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2), a AS (SELECT attname, atttypid from pg_catalog.pg_attribute WHERE attrelid = $3::regclass::oid) SELECT * FROM c LEFT JOIN a ON c.column_name = a.attname;";
    const char* findPrimaryKeysSql = "SELECT c.column_name, cc.ordinal_position FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name AND c.table_schema = t.table_schema AND c.table_name = t.table_name LEFT JOIN information_schema.columns cc ON c.table_schema = cc.table_schema AND c.table_name = cc.table_name AND c.column_name = cc.column_name WHERE t.table_schema = $1 AND t.table_name = $2 AND t.constraint_type = 'PRIMARY KEY'";
    const char* findDistributionKeysSql = "WITH d AS (SELECT table_namespace, table_name, unnest(string_to_array(property_value, ',')) as column_name from hologres.hg_table_properties WHERE table_namespace = $1 AND table_name = $2 AND property_key = 'distribution_key') SELECT c.column_name, c.ordinal_position FROM d LEFT JOIN information_schema.columns c ON d.table_namespace = c.table_schema AND d.table_name=c.table_name AND d.column_name = c.column_name";
    const char* findPartitionColumnSql = "SELECT partattrs FROM pg_partitioned_table WHERE partrelid = $1::regclass::oid";
    PGresult* res = NULL;
    int nTuples, i, pos = 0;
    char oid[11];
    // use prepared statement, so there's no need to quote_literal_cstr() before use
    const char* name[1] = {tableName.fullName};
    const char* names[2] = {tableName.schemaName, tableName.tableName};
    const char* names3[3] = {tableName.schemaName, tableName.tableName, tableName.fullName};

    //get table oid
    res = connection_holder_exec_params_with_retry(connHolder, findTableOidSql, 2, NULL, names, NULL, NULL, 0, errMsgAddr);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0){
        LOG_ERROR("Get table Oid of table %s failed.", tableName.fullName);
        holo_client_destroy_tableschema(schema);
        if (res != NULL) PQclear(res);
        return FAILURE_NOT_NEED_RETRY;
    } else {
        schema->tableId = atoi(PQgetvalue(res, 0, 0));
    }
    if (res != NULL) PQclear(res);

    //get column_name, data_type_oid, is_nullable, default_value of each column
    sprintf(oid, "%d", schema->tableId);
    res = connection_holder_exec_params_with_retry(connHolder, findColumnsSql, 3, NULL, names3, NULL, NULL, 0, errMsgAddr);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK  || PQntuples(res) == 0){
        LOG_ERROR("Get column info of table %s failed.", tableName.fullName);
        if (res != NULL) PQclear(res);
        return FAILURE_NOT_NEED_RETRY;
    } else {
        nTuples = PQntuples(res);
        schema->nColumns = nTuples;
        columns = holo_client_new_columns(nTuples);
        for (i = 0; i < nTuples; i++) {
            pos = atoi(PQgetvalue(res, i, 1)) - 1;
            if (pos >= 0 && pos < nTuples) {
                columns[pos].name = deep_copy_string(PQgetvalue(res, i, 0));
                columns[pos].quoted = quote_identifier(columns[pos].name);
                columns[pos].type = atoi(PQgetvalue(res, i, 5));
                if (strcmp(PQgetvalue(res, i, 2), "YES") == 0) {
                    columns[pos].nullable = true;
                } else {
                    columns[pos].nullable = false;
                }
                columns[pos].isPrimaryKey = false;
                if (PQgetisnull(res, i, 3)) {
                    columns[pos].defaultValue = NULL;
                } else {
                    columns[pos].defaultValue = deep_copy_string(PQgetvalue(res, i, 3));
                }
            }
        }
        schema->columns = columns;
    }
    if (res != NULL) PQclear(res);

    //find primary keys
    res = connection_holder_exec_params_with_retry(connHolder, findPrimaryKeysSql, 2, NULL, names, NULL, NULL, 0, errMsgAddr);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK){
        LOG_ERROR("Get primary keys info of table %s failed.", tableName.fullName);
        holo_client_destroy_tableschema(schema);
        if (res != NULL) PQclear(res);
        return FAILURE_NOT_NEED_RETRY;
    } else {
        nTuples = PQntuples(res);
        schema->nPrimaryKeys = nTuples;
        FREE(schema->primaryKeys);
        if (nTuples > 0){
            schema->primaryKeys = MALLOC(nTuples, int);
        }
        for (i = 0; i < nTuples; i++) {
            pos = atoi(PQgetvalue(res, i, 1)) - 1;
            if (pos >= 0 && pos < schema->nColumns) {
                columns[pos].isPrimaryKey = true;
                schema->primaryKeys[i] = pos;
            }
        }
    }
    if (res != NULL) PQclear(res);

    //find distribution keys
    res = connection_holder_exec_params_with_retry(connHolder, findDistributionKeysSql, 2, NULL, names, NULL, NULL, 0, errMsgAddr);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK){
        LOG_ERROR("Get distribution keys info of table %s failed.", tableName.fullName);
        holo_client_destroy_tableschema(schema);
        if (res != NULL) PQclear(res);
        return FAILURE_NOT_NEED_RETRY;
    } else {
        nTuples = PQntuples(res);
        schema->nDistributionKeys = nTuples;
        if (nTuples > 0) {
            schema->distributionKeys = MALLOC(nTuples,  int);
        }
        for (i = 0; i < nTuples; i++) {
            pos = atoi(PQgetvalue(res, i, 1)) - 1;
            if (pos >= 0 && pos < schema->nColumns) {
                schema->distributionKeys[i] = pos;
            }
        }
    }
   if (res != NULL) PQclear(res);

   //find partition column
    res = connection_holder_exec_params_with_retry(connHolder, findPartitionColumnSql, 1, NULL, name, NULL, NULL, 0, errMsgAddr);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK){
        LOG_ERROR("Get partition column of table %s failed.", tableName.fullName);
        holo_client_destroy_tableschema(schema);
        if (res != NULL) PQclear(res);
        return FAILURE_NOT_NEED_RETRY;
    } else if (PQntuples(res) == 0) {
        schema->partitionColumn = -1;
    }
    else {
        schema->partitionColumn = atoi(PQgetvalue(res, 0, 0)) - 1;
    }
    if (res != NULL) PQclear(res);

    //deep copy table name 
    schema->tableName->fullName = deep_copy_string(tableName.fullName);
    schema->tableName->schemaName = deep_copy_string(tableName.schemaName);
    schema->tableName->tableName = deep_copy_string(tableName.tableName);

    return SUCCESS;
}

ActionStatus handle_mutations(ConnectionHolder* connHolder, dlist_head* mutations, char** errMsgAddr){
    if (dlist_is_empty(mutations)) return FAILURE_NOT_NEED_RETRY;

    dlist_head insertBatchList;
    dlist_init(&insertBatchList);
    dlist_head deleteBatchList;
    dlist_init(&deleteBatchList);

    dlist_mutable_iter miterMutation;
    MutationItem* mutationItem;
    dlist_foreach_modify(miterMutation, mutations) {
        mutationItem = dlist_container(MutationItem, list_node, miterMutation.cur);
        if (mutationItem->mutation->mode == DELETE){
            add_mutation_item_to_batch_list(mutationItem, &deleteBatchList);
        } else {
            add_mutation_item_to_batch_list(mutationItem, &insertBatchList);
        }
    }

    ActionStatus rc = SUCCESS;
    rc = handle_batch_list(&deleteBatchList, connHolder, errMsgAddr) != SUCCESS ? FAILURE_NOT_NEED_RETRY : rc;
    rc = handle_batch_list(&insertBatchList, connHolder, errMsgAddr) != SUCCESS ? FAILURE_NOT_NEED_RETRY : rc;

    return rc;
}

void add_mutation_item_to_batch_list(MutationItem* mutationItem, dlist_head* batchList) {
    dlist_mutable_iter miterBatch;
    BatchItem* batchItem;
    bool applied = false;
    dlist_foreach_modify(miterBatch, batchList) {
        batchItem = dlist_container(BatchItem, list_node, miterBatch.cur);
        if (batch_try_apply_mutation_request(batchItem->batch, mutationItem->mutation)) {
            applied = true;
            break;
        }
    }
    if (!applied){
        Batch* newBatch = holo_client_new_batch_with_mutation_request(mutationItem->mutation);
        dlist_push_tail(batchList, &(create_batch_item(newBatch)->list_node));
    }
}

ActionStatus handle_batch_list(dlist_head* batchList, ConnectionHolder* connHolder, char** errMsgAddr) {
    if (dlist_is_empty(batchList)) {
        return SUCCESS;
    }

    dlist_mutable_iter miterBatch;
    BatchItem* batchItem;
    ActionStatus rc = SUCCESS;

    dlist_foreach_modify(miterBatch, batchList) {
        batchItem = dlist_container(BatchItem, list_node, miterBatch.cur);
        if (!is_batch_support_unnest(connHolder, batchItem->batch)) {
            int maxSize = get_max_pow(batchItem->batch->nRecords);
            // make sure that num of parameters in one batch is less than MAX_PARAM_NUM in PG
            while (maxSize * batchItem->batch->nValues > MAX_PARAM_NUM) {
                maxSize >>= 1;
            }
            rc = handle_batch(batchItem->batch, maxSize, exec_batch, connHolder, errMsgAddr);
        } else {
            batchItem->batch->isSupportUnnest = true;
            rc = handle_batch(batchItem->batch, batchItem->batch->nRecords, exec_batch, connHolder, errMsgAddr);
        }
        holo_client_destroy_batch(batchItem->batch);
        dlist_delete(miterBatch.cur);
        FREE(batchItem);
    }

    return rc;
}

ActionStatus handle_batch(Batch* batch, int maxSize ,BatchHandler do_handle_batch , ConnectionHolder* connHolder, char** errMsgAddr){
    ActionStatus rc = SUCCESS;
    dlist_node* current = dlist_head_node(&batch->recordList);
    int remainRecords = batch->nRecords;
    while (remainRecords != 0){
        int nRecords = maxSize;
        if (remainRecords < maxSize){
            nRecords = 1;
            int tRecords = remainRecords;
            while ((tRecords >>= 1) != 0) nRecords <<= 1;
            remainRecords -= nRecords;
        }
        else remainRecords -= maxSize;
        ActionStatus t = do_handle_batch(connHolder, batch, &current, nRecords, errMsgAddr);
        if (t != SUCCESS) rc = FAILURE_NOT_NEED_RETRY;
    }
    return rc;
}

int get_val_len_by_type_oid(unsigned int typeOid) {
    switch (typeOid)
    {
    case HOLO_TYPE_INT4:
    case HOLO_TYPE_FLOAT4:
        return 4;
    case HOLO_TYPE_INT8:
    case HOLO_TYPE_FLOAT8:
    case HOLO_TYPE_TIMESTAMP:
    case HOLO_TYPE_TIMESTAMPTZ:
        return 8;
    case HOLO_TYPE_INT2:
        return 2;
    case HOLO_TYPE_BOOL:
        return 1;
    default:
        LOG_ERROR("Varlena type cannot get fixed value length.");
        break;
    }
    return -1;
}

unsigned int get_array_oid_by_type_oid(unsigned int typeOid) {
    switch (typeOid)
    {
    case HOLO_TYPE_INT4:
        return HOLO_TYPE_INT4_ARRAY;
    case HOLO_TYPE_INT8:
        return HOLO_TYPE_INT8_ARRAY;
    case HOLO_TYPE_INT2:
        return HOLO_TYPE_INT2_ARRAY;
    case HOLO_TYPE_BOOL:
        return HOLO_TYPE_BOOL_ARRAY;
    case HOLO_TYPE_FLOAT4:
        return HOLO_TYPE_FLOAT4_ARRAY;
    case HOLO_TYPE_FLOAT8:
        return HOLO_TYPE_FLOAT8_ARRAY;
    case HOLO_TYPE_TIMESTAMP:
        return HOLO_TYPE_TIMESTAMP_ARRAY;
    case HOLO_TYPE_TIMESTAMPTZ:
        return HOLO_TYPE_TIMESTAMPTZ_ARRAY;
    case HOLO_TYPE_CHAR:
        return HOLO_TYPE_CHAR_ARRAY;
    case HOLO_TYPE_VARCHAR:
        return HOLO_TYPE_VARCHAR_ARRAY;
    case HOLO_TYPE_TEXT:
        return HOLO_TYPE_TEXT_ARRAY;
    case HOLO_TYPE_BYTEA:
        return HOLO_TYPE_BYTEA_ARRAY;
    case HOLO_TYPE_JSON:
        return HOLO_TYPE_JSON_ARRAY;
    case HOLO_TYPE_JSONB:
        return HOLO_TYPE_JSONB_ARRAY;
    case HOLO_TYPE_DATE:
        return HOLO_TYPE_DATE_ARRAY;
    case HOLO_TYPE_NUMERIC:
        return HOLO_TYPE_NUMERIC_ARRAY;
    default:
        LOG_ERROR("Unsupported array type, origin type: %d.", typeOid);
        break;
    }
    return 0;
}

int get_convert_mode_for_unnest(Batch* batch, int colIdx) {
    int convertMode = 0;
    switch (batch->schema->columns[colIdx].type)
    {
    case HOLO_TYPE_TIMESTAMP:
    case HOLO_TYPE_TIMESTAMPTZ:
        if (batch->valueFormats[colIdx] == 1) {
            convertMode = 1;
        } else {
            convertMode = 3;
        }
        break;
    case HOLO_TYPE_INT4:
    case HOLO_TYPE_INT8:
    case HOLO_TYPE_INT2:
    case HOLO_TYPE_BOOL:
    case HOLO_TYPE_FLOAT4:
    case HOLO_TYPE_FLOAT8:
        convertMode = 1;
        break;
    case HOLO_TYPE_CHAR:
    case HOLO_TYPE_VARCHAR:
    case HOLO_TYPE_TEXT:
    case HOLO_TYPE_JSON:
    case HOLO_TYPE_JSONB:
        convertMode = 3;
        break;
    case HOLO_TYPE_BYTEA:
    case HOLO_TYPE_NUMERIC:
    case HOLO_TYPE_DATE:
        convertMode = 3;
        break;
    default:
        LOG_ERROR("Generate convertMode failed for unnest, type is %d.", batch->schema->columns[colIdx].type);
        break;
    }
    return convertMode;
}

ActionStatus exec_batch(ConnectionHolder* connHolder, Batch* batch, dlist_node** current, int nRecords, char** errMsgAddr){
    if (batch->nRecords <= 0){
        LOG_WARN("Nothing to insert.");
        return FAILURE_NOT_NEED_RETRY;
    }
    if (batch->nRecords == 1){
        LOG_DEBUG("Single record in batch.");
        return exec_batch_one_by_one(connHolder, batch, current, nRecords, errMsgAddr);
    }

    if (nRecords == 0) nRecords = batch->nRecords;
    SqlCache* sqlCache = connection_holder_get_or_create_sql_cache_with_batch(connHolder, batch, nRecords);
    int nParams;
    char** params;
    dlist_mutable_iter miter;
    miter.cur = *current;
    RecordItem* recordItem;
    int64_t minSeq = INT64_MAX;
    int64_t maxSeq = INT64_MIN;
    if (batch->isSupportUnnest && nRecords > 1) {
        nParams = batch->nValues;
        params = MALLOC(nParams, char*);
        int cParam = 0;
        for (int i = 0; i < batch->schema->nColumns; i++) {
            if (!batch->valuesSet[i]) continue;
            // 对于每个set value的列，创建数组
            char** valueArray = MALLOC(nRecords, char*);
            int* lengthArray = MALLOC(nRecords, int);
            // 遍历recordList，取对应位置的value填充数组
            int cRecords = 0;
            dlist_foreach_from(miter, &(batch->recordList), *current) {
                if (cRecords >= nRecords) break;
                recordItem = dlist_container(RecordItem, list_node, miter.cur);
                lengthArray[cRecords] = recordItem->record->valueLengths[i];
                valueArray[cRecords++] = recordItem->record->values[i];
                minSeq = recordItem->record->sequence < minSeq ? recordItem->record->sequence : minSeq;
                maxSeq = recordItem->record->sequence > maxSeq ? recordItem->record->sequence : maxSeq;
            }
            // 把这个数组变成一条record，插入param
            int length = 0;
            char* ptr = NULL;
            int convertMode = get_convert_mode_for_unnest(batch, i);

            switch (convertMode)
            {
            case 1:
                // binary转成binary写入
                length = 20 + 4 * nRecords;
                for (int j = 0; j < nRecords; j++) {
                    if (valueArray[j] == NULL) {
                        continue;
                    }
                    length += get_val_len_by_type_oid(batch->schema->columns[i].type);
                }
                ptr = MALLOC(length, char);
                unnest_convert_array_to_postgres_binary(ptr, valueArray, nRecords, get_val_len_by_type_oid(batch->schema->columns[i].type), batch->schema->columns[i].type);
                sqlCache->paramTypes[cParam] = get_array_oid_by_type_oid(batch->schema->columns[i].type);
                sqlCache->paramFormats[cParam] = 1;
                params[cParam] = ptr;
                sqlCache->paramLengths[cParam++] = length;
                FREE(valueArray);
                FREE(lengthArray);
                break;
            case 2:
                // text转成binary写入
                length = 20 + 4 * nRecords;
                for (int j = 0; j < nRecords; j++) {
                    if (valueArray[j] == NULL) {
                        continue;
                    }
                    length += strlen(valueArray[j]);
                }
                ptr = MALLOC(length, char);
                convert_text_array_to_postgres_binary(ptr, valueArray, nRecords, batch->schema->columns[i].type);
                sqlCache->paramTypes[cParam] = get_array_oid_by_type_oid(batch->schema->columns[i].type);
                sqlCache->paramFormats[cParam] = 1;
                params[cParam] = ptr;
                sqlCache->paramLengths[cParam++] = length;
                FREE(valueArray);
                FREE(lengthArray);
                break;
            case 3:
                // text转成text写入，参考pg jdbc的拼法
                length = unnest_convert_array_to_text(&ptr, valueArray, lengthArray, nRecords);
                sqlCache->paramTypes[cParam] = get_array_oid_by_type_oid(batch->schema->columns[i].type);
                sqlCache->paramFormats[cParam] = 0;
                params[cParam] = ptr;
                sqlCache->paramLengths[cParam++] = length;
                FREE(valueArray);
                FREE(lengthArray);
                break;
            default:
                LOG_ERROR("Unknown convertMode in unnest");
                break;
            }
        }
    } else {
        nParams = nRecords * batch->nValues;
        params = MALLOC(nParams, char*);
        int count = -1;
        int cRecords = 0;
        dlist_foreach_from(miter, &(batch->recordList), *current){
            if (cRecords >= nRecords) break;
            recordItem = dlist_container(RecordItem, list_node, miter.cur);
            for (int i = 0;i < batch->schema->nColumns;i++){
                if (!batch->valuesSet[i]) continue;
                params[++count] = recordItem->record->values[i];
                sqlCache->paramLengths[count] = recordItem->record->valueLengths[i];
            }
            minSeq = recordItem->record->sequence < minSeq ? recordItem->record->sequence : minSeq;
            maxSeq = recordItem->record->sequence > maxSeq ? recordItem->record->sequence : maxSeq;
            cRecords++;
        }
    }

    PGresult* res = NULL;
    res = connection_holder_exec_params_with_retry(connHolder, sqlCache->command, nParams, sqlCache->paramTypes, (const char* const*)params, sqlCache->paramLengths, sqlCache->paramFormats, 0, errMsgAddr);
    if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK){
        LOG_ERROR("Mutate into table \"%s\" as batch failed.", batch->schema->tableName->tableName);

        ActionStatus rc = SUCCESS;
        if (batch->isSupportUnnest && nRecords > 1 && nRecords * batch->nValues <= MAX_PARAM_NUM) {
            LOG_WARN("Retrying once without unnest...");
            batch->isSupportUnnest = false;
            rc = exec_batch(connHolder, batch, current, nRecords, errMsgAddr);
            if (res != NULL) PQclear(res);
            for (int i = 0; i < nParams; i++) {
                FREE(params[i]);
            }
            FREE(params);
            return rc;
        }
        if (res != NULL && is_dirty_data_error(get_errcode_from_pg_res(res))) {
            LOG_WARN("Retrying one by one...");
            //脏数据类型的异常，需要拆成一条一条重试
            rc = exec_batch_one_by_one(connHolder, batch, current, nRecords, errMsgAddr);
        } else {
            //对于不one by one的情况，也要回调做异常处理，但是不提供record，只给出errMsg。用户可以通过record指针是否为NULL自行设计逻辑
            connHolder->handleExceptionByUser(NULL, PQresultErrorMessage(res), connHolder->exceptionHandlerParam);
            rc = FAILURE_NOT_NEED_RETRY;
        }
        if (res != NULL) PQclear(res);
        if (batch->isSupportUnnest && nRecords > 1) {
            for (int i = 0; i < nParams; i++) {
                FREE(params[i]);
            }
        }
        FREE(params);
        return rc;
    }
    LOG_DEBUG("Mutate into table \"%s\" as batch succeed, minSeq:%"PRId64", maxSeq:%"PRId64".", batch->schema->tableName->tableName, minSeq, maxSeq);

    metrics_meter_mark(connHolder->metrics->rps, nRecords);
    metrics_meter_mark(connHolder->metrics->qps, 1);
    if (res != NULL) PQclear(res);
    if (batch->isSupportUnnest && nRecords > 1) {
        for (int i = 0; i < nParams; i++) {
            FREE(params[i]);
        }
    }
    FREE(params);
    *current = miter.cur;

    return SUCCESS;
}

ActionStatus exec_batch_one_by_one(ConnectionHolder* connHolder, Batch* batch, dlist_node** current, int nRecords, char** errMsgAddr){
    if (batch->nRecords <= 0){
        LOG_WARN("Nothing to insert.");
        return FAILURE_NOT_NEED_RETRY;
    }
    
    if (nRecords == 0) nRecords = batch->nRecords;
    SqlCache* sqlCache = connection_holder_get_or_create_sql_cache_with_batch(connHolder, batch, 1);

    PGresult* res = NULL;
    char** params = MALLOC(batch->nValues, char*);
    dlist_mutable_iter miter;
    RecordItem* recordItem;
    ActionStatus rc = SUCCESS;
    int cRecords = 0;
    int64_t minSeq = INT64_MAX;
    int64_t maxSeq = INT64_MIN;
    dlist_foreach_from(miter, &(batch->recordList), *current){
        if (cRecords >= nRecords) break;
        recordItem = dlist_container(RecordItem, list_node, miter.cur);
        int count = -1;
        for (int i = 0;i < batch->schema->nColumns;i++){
            if (!batch->valuesSet[i]) continue;
            params[++count] = recordItem->record->values[i];
            sqlCache->paramLengths[count] = recordItem->record->valueLengths[i];
        }
        minSeq = recordItem->record->sequence < minSeq ? recordItem->record->sequence : minSeq;
        maxSeq = recordItem->record->sequence > maxSeq ? recordItem->record->sequence : maxSeq;
        res = connection_holder_exec_params_with_retry(connHolder, sqlCache->command, batch->nValues, sqlCache->paramTypes, (const char* const*)params, sqlCache->paramLengths, sqlCache->paramFormats, 0, errMsgAddr);
        if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK){
            LOG_ERROR("Mutate into table \"%s\" failed.", batch->schema->tableName->tableName);
            connHolder->handleExceptionByUser(recordItem->record, PQresultErrorMessage(res), connHolder->exceptionHandlerParam);
            rc = FAILURE_NOT_NEED_RETRY;
        }
        if (res != NULL) PQclear(res);
        cRecords++;
    }

    LOG_DEBUG("Mutate into table \"%s\" one by one succeed, minSeq:%"PRId64", maxSeq:%"PRId64".", batch->schema->tableName->tableName, minSeq, maxSeq);

    metrics_meter_mark(connHolder->metrics->rps, nRecords);
    metrics_meter_mark(connHolder->metrics->qps, 1);
    FREE(params);
    *current = miter.cur;

    return rc;
}

ActionStatus handle_sql(ConnectionHolder* connHolder, SqlFunction sqlFunction, void* arg, void** retAddr) {
    connection_holder_exec_func_with_retry(connHolder, sqlFunction, arg, retAddr);
    return SUCCESS;
}

int res_tuple_hashcode(PGresult* res, int n, HoloTableSchema* schema, int size) {
    unsigned raw = 0;
    bool first = true;
    for (int i = 0;i < schema->nPrimaryKeys;i++){
        int index = schema->primaryKeys[i];
        char* value = PQgetvalue(res, n, index);
        int length = strlen(value) + 1;
        if (first){
            MurmurHash3_x86_32(value, length, 0xf7ca7fd2, &raw);
            first = false;
        }
        else{
            unsigned t = 0;
            MurmurHash3_x86_32(value, length, 0xf7ca7fd2, &t);
            raw ^= t;
        }
    }
    int hash = raw % ((unsigned)65536);
    int base = 65536 / size;
    int remain = 65536 % size;
    int pivot = (base + 1) * remain;
    int index = 0;
    if (hash < pivot) index = hash / (base + 1);
    else index = (hash - pivot) / base + remain;
    return index;
}

bool res_tuple_equals(PGresult* res, int n1, int n2, HoloTableSchema* schema) {
    for (int i = 0;i < schema->nPrimaryKeys;i++){
        int index = schema->primaryKeys[i];
        char* v1 = PQgetvalue(res, n1, index);
        char* v2 = PQgetvalue(res, n2, index);
        if (strcmp(v1, v2) != 0) return false;
    }
    return true;
}

void res_tuple_to_map(PGresult* res, int n, HoloTableSchema* schema, LPMap* map, int maxSize) {
    int index = res_tuple_hashcode(res, n, schema, maxSize);
    int M = maxSize;
    for(int i = 0; i < M; i++) {
        if(map->values[index] == NULL) {
            map->values[index] = (void*)(long)(n+1);
            map->size++;
            return;
        }
        if(res_tuple_equals(res, n, ((intptr_t)map->values[index])-1, schema)) {
            return;
        }
        index = (index + 1) % M;
    }
}

ActionStatus handle_gets(ConnectionHolder* connHolder, HoloTableSchema* schema, dlist_head* gets, int nRecords) {
    SqlCache* sqlCache = connection_holder_get_or_create_get_sql_cache(connHolder, schema, nRecords);
    int nParams = nRecords * schema->nPrimaryKeys;
    char** params = MALLOC(nParams, char*);
    dlist_iter iter;
    GetItem* getItem;
    int count = -1;
    dlist_foreach(iter, gets){
        getItem = dlist_container(GetItem, list_node, iter.cur);
        for (int i = 0;i < schema->nPrimaryKeys;i++){
            int col = schema->primaryKeys[i];
            params[++count] = getItem->get->record->values[col];
            sqlCache->paramLengths[count] = getItem->get->record->valueLengths[col];
            sqlCache->paramFormats[count] = getItem->get->record->valueFormats[col];
        }
    }

    PGresult* res = NULL;
    res = connection_holder_exec_params_with_retry(connHolder, sqlCache->command, nParams, sqlCache->paramTypes, (const char* const*)params, sqlCache->paramLengths, sqlCache->paramFormats, 0, NULL);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK){
        LOG_ERROR("Get from table \"%s\" as batch failed.", schema->tableName->tableName);
        if (res != NULL) {
            LOG_ERROR("Error msg: %s", PQresultErrorMessage(res));
            PQclear(res);
        }
        FREE(params);
        return FAILURE_NOT_NEED_RETRY;
    }
    metrics_meter_mark(connHolder->metrics->rps, nRecords);
    metrics_meter_mark(connHolder->metrics->qps, 1);

    LPMap* map = connHolder->map;
    int nTuples = PQntuples(res);
    int M = nTuples > 0 ? nTuples * 2 : 1;
    for (int i = 0; i < nTuples; i++) {
        res_tuple_to_map(res, i, schema, map, M);
    }
    dlist_foreach(iter, gets){
        int resNum = -1;
        getItem = dlist_container(GetItem, list_node, iter.cur);
        int index = record_pk_hash_code(getItem->get->record, M);
        
        for (int i = 0; i < M; i++){
            if(map->values[index] == NULL) break;
            bool same = true;
            int temp = ((intptr_t)map->values[index]) - 1;
            for (int j = 0; j < schema->nPrimaryKeys; j++) {
                int col = schema->primaryKeys[j];
                if (strcmp(getItem->get->record->values[col], PQgetvalue(res, temp, col)) != 0) {
                    same = false;
                    break;
                }
            }
            if (same) {
                resNum = temp;
                break;
            }
            index = (index + 1) % M;
        }

        if (resNum == -1) {
            complete_future(getItem->get->future, NULL);
        } else {
            HoloRecord* resRecord = holo_client_new_record(schema);
            for (int n = 0; n < schema->nColumns; n++) {
                char* value = PQgetvalue(res, resNum, n);
                int len = strlen(value);
                char* ptr = (char*)new_record_val(resRecord, len + 1);
                deep_copy_string_to(value, ptr, len + 1);
                set_record_val(resRecord, n, ptr, 0, len + 1);
            }
            complete_future(getItem->get->future, resRecord);
        }
    }

    if (res != NULL) PQclear(res);
    FREE(params);
    holo_client_clear_lp_map(map);

    return SUCCESS;
}