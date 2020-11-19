create or replace procedure SP_ASYNC_BATCH(I_METHOD VARCHAR,I_METHOD_PARAM_1 VARCHAR, I_METHOD_PARAM_2 VARCHAR)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$
// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-10-18
// Purpose     SP_CONCURRENT is a "library" of functions to perform the work for a embarrassingly parallel
//             problem. 
//             The stored procedure accepts the following methods:
// 
//                CONF: 
//                   Parameters:
//                       I_METHOD: 'CONF'
//                       I_METHOD_PARAM_1: Concordance table 
//
//                POST: 
//                   Parameters:
//                       I_METHOD: 'POST'
//                       I_METHOD_PARAM_1: Concordance table
//                       I_METHOD_PARAM_2: db.schema.external_function
//
//
//                GET: 
//                   Parameters:
//                       I_METHOD: 'GET'
//                       I_METHOD_PARAM_1: Concordance table
//                       I_METHOD_PARAM_2: db.schema.external_function
//
//             Process coordination is accomplished via the SCHEDULER and the LOG tables.
//
//             The statements to be executed are store in JSON format in the input table reference passed as METHOD_PARAMETER_4 
//                 of the PROCESS_REQUEST call. 
// -----------------------------------------------------------------------------
// Modification History
//
// 2020-11-04 Robert Fehrmann  
//      Initial Version
// -----------------------------------------------------------------------------
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

const TIMEZONE='UTC';

// copy parameters into local constant; none of the input values can be modified
const METHOD= I_METHOD;
const CONCORDANCE_TABLE=I_METHOD_PARAM_1;
const EXTERNAL_FUNCTION=I_METHOD_PARAM_2;


// keep all constants in a common place
const METHOD_CONF="CONF";
const METHOD_POST="POST";
const METHOD_GET="GET";

const REQUEST_TYPE_TASK="TASK";
const REQUEST_TYPE_DECISION="DECISION"

const STATUS_REQUESTED="REQUESTED";
const STATUS_PENDING="PENDING";
const STATUS_COMPLETED="COMPLETED";
const STATUS_MAPPED="MAPPED";
const STATUS_REVIEW="REVIEW";

const STATUS_BEGIN= "BEGIN";
const STATUS_END = "END";
const STATUS_WARNING = "WARNING";
const STATUS_FAILURE = "FAILURE";

const CONCORDANCE_INTERFACE_TABLE=CONCORDANCE_TABLE+"_INTERFACE";
const CONCORDANCE_INTERFACE_TABLE_STATUS=CONCORDANCE_INTERFACE_TABLE+"_STATUS";
const CONCORDANCE_TABLE_STREAM=CONCORDANCE_TABLE+"_STREAM";
const CONCORDANCE_INTERFACE_TABLE_STREAM=CONCORDANCE_INTERFACE_TABLE+"_STREAM";

const CONCORDANCE_INTERFACE=CONCORDANCE_TABLE+"_INTERFACE";

// Global Variables
var current_database="";
var current_schema="";
var current_warehouse="";
var return_array = [];
var current_session_id=0;
var do_log=false;
var log_table="";

var this_name = Object.keys(this)[0];
var procName = this_name + "-" + METHOD;

// -----------------------------------------------------------------------------
//  log a debug message in the results array
// -----------------------------------------------------------------------------
function log ( msg ) {
    if  (do_log!==true) return;
    var d=new Date();
    var UTCTimeString=("00"+d.getUTCHours()).slice(-2)+":"+("00"+d.getUTCMinutes()).slice(-2)+":"+("00"+d.getUTCSeconds()).slice(-2);
    return_array.push(UTCTimeString+" "+msg);
}

// -----------------------------------------------------------------------------
//  persist all debug messages in the results array into one row in the log table 
// -----------------------------------------------------------------------------
function flush_log (status){
    var message="";
    var sqlquery="";

    for (i=0; i < return_array.length; i++) {
        message=message+String.fromCharCode(13)+return_array[i];
    }
    message=message.replace(/'/g,""); //' keep formatting in VS nice

    if (log_table=="") return;
    var sqlquery = "INSERT INTO IDENTIFIER("+log_table+") ( method, status,message) values ";
    sqlquery = sqlquery + "('" + METHOD + "','" + status + "','" + message + "');";
    snowflake.execute({sqlText: sqlquery});
}

// -----------------------------------------------------------------------------
//  read environment values and set up logging
//    logging is turned off by default; enable logging by 
//       set do_log=true
//          The stored proc returns an array of logging message
//       log_table=<your log table>
//          The array of log messages is also persistet in <your log table>
// -----------------------------------------------------------------------------
function init () {
    var sqlquery="";

    sqlquery=`
        SELECT current_warehouse(),current_database(),current_schema(),current_session()
    `;
    snowflake.execute({sqlText: sqlquery});

    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    if (ResultSet.next()) {
        current_warehouse=ResultSet.getColumnValue(1);
        current_database=ResultSet.getColumnValue(2);
        current_schema=ResultSet.getColumnValue(3);
        current_session_id=ResultSet.getColumnValue(4);
    } else {
        throw new Error ("INIT FAILURE");
    }
    try {
        var stmt = snowflake.createStatement( { sqlText: `select $do_log` } ).execute();
        stmt.next();
        do_log=stmt.getColumnValue(1);
    } catch (ERROR){
        do_log=true;
        return; //swallow the error, variable not set so dont log
    }
    log(do_log.toString())
    if (do_log==true) {
        try {
            var stmt = snowflake.createStatement( { sqlText: `select $log_table` } ).execute();
            stmt.next();
            log_table=stmt.getColumnValue(1);
            try {
                sqlquery=`
                    CREATE TEMP TABLE IF NOT EXISTS IDENTIFER(`+log_table+`) (
                        id integer AUTOINCREMENT (0,1)
                        ,create_ts timestamp_tz(9) default convert_timezone('`+TIMEZONE+`',current_timestamp)
                        ,session_id number default to_number(current_session())
                        ,method varchar
                        ,status varchar
                        ,message varchar)`;
                snowflake.execute({sqlText: sqlquery});            
            } catch (ERROR){
                throw ERROR;
            } 
        } catch (ERROR){
            log_table="";
            return; //swallow the error, variable not set so dont log
        }
    }
}

// -----------------------------------------------------------------------------
//  this functions creates all tables, views, streams required 
// -----------------------------------------------------------------------------
function concordance_configure() {
    log ("CONFIGURE")
    
    sqlquery=`
        CREATE TABLE `+CONCORDANCE_TABLE+` (
            id integer identity (0,1)
            ,name varchar 
            ,country varchar 
            ,state varchar
            ,website varchar
            ,map_status varchar
            ,entity_id varchar
            ,status varchar default '`+STATUS_REQUESTED+`'
            ,last_modified_ts timestamp default current_timestamp()
            ,create_ts timestamp default current_timestamp())
    `;
    snowflake.execute({sqlText: sqlquery});

    sqlquery=`
        CREATE OR REPLACE STREAM `+CONCORDANCE_TABLE_STREAM+` ON TABLE `+CONCORDANCE_TABLE+`
    `;
    snowflake.execute({sqlText: sqlquery});

    sqlquery=`
        CREATE OR REPLACE TABLE `+CONCORDANCE_INTERFACE_TABLE+` (
            id integer identity (0,1)
            ,request_type varchar not null
            ,name varchar 
            ,country varchar 
            ,state varchar
            ,website varchar
            ,task_id varchar
            ,task_index varchar
            ,status varchar
            ,map_status varchar
            ,entity_id varchar
            ,concordance variant
            ,create_ts timestamp default current_timestamp())
    `;
    snowflake.execute({sqlText: sqlquery});

    sqlquery=`
        CREATE OR REPLACE STREAM `+CONCORDANCE_INTERFACE_TABLE_STREAM+` ON TABLE `+CONCORDANCE_INTERFACE_TABLE+`
    `;
    snowflake.execute({sqlText: sqlquery});

    sqlquery=`
        CREATE OR REPLACE VIEW `+CONCORDANCE_INTERFACE_TABLE_STATUS+` AS
            WITH tasks AS (
                SELECT *
                FROM `+CONCORDANCE_INTERFACE_TABLE+` 
                QUALIFY 1=(row_number() over (partition by name,country,state,website order by create_ts desc))
            ) 
            SELECT * 
            FROM tasks
            ORDER BY task_id::int,task_index::int
    `;
    snowflake.execute({sqlText: sqlquery});
}

// -----------------------------------------------------------------------------
//  parse the input parameter into 3 parts, 
//    i.e. database, schema, and object 
// -----------------------------------------------------------------------------
function parse_path(ipath) {
    var path=ipath.split('.');
    var result="";
    if (path.length==3) {
        result=path[0]+'.'+path[1]+'.'+path[2];
    } else {
        if (path.length==2) {
            result=current_database+'.'+path[1]+'.'+path[2];
        } else {
            result==current_database+'.'+current_schema+'.'+path[2];
        }
    }
    return result; 
}
// -----------------------------------------------------------------------------
// read tuple (name,country,state,website) from the input table and 
//   request a company match by calling the FACTSET Task API (batch). The API
//   returns a Task ID and rowIndex for each tupel
// -----------------------------------------------------------------------------
function concordance_task_post(external_function) {
    const FULLY_QUALIFIED_PATH=parse_path(external_function);

    log("CREATE REQUESTS")

    sqlquery=`
        INSERT INTO `+CONCORDANCE_INTERFACE_TABLE+`
                (request_type,name,country,state,website,task_id, task_index,status, concordance )
            SELECT '`+REQUEST_TYPE_TASK+`' request_type
                ,concordance:"name"::varchar requested_name
                ,concordance:"country"::varchar requested_country
                ,concordance:"state"::varchar requested_state
                ,concordance:"url"::varchar requested_url
                ,concordance:"taskId"::varchar task_id
                ,concordance:"rowIndex"::int task_index
                ,concordance:"taskStatus"::varchar status
                ,concordance
            FROM (
                SELECT `+FULLY_QUALIFIED_PATH+`(name, country, state,website)[0] concordance
                FROM  `+CONCORDANCE_TABLE_STREAM+`
                WHERE STATUS='`+STATUS_REQUESTED+`'
                    AND NAME is not null 
                    AND METADATA$ACTION='INSERT')
    `;
    snowflake.execute({sqlText: sqlquery});


}

// -----------------------------------------------------------------------------
// for all tupel still in status PENDING, request a decision from the FACTSET
//    decision API (batch); 
// for all tupel with a decision, i.e. status is no longer PENDING, merge the 
//    decision into the input table 
// -----------------------------------------------------------------------------
function concordance_task_get(external_function) {
    const FULLY_QUALIFIED_PATH=parse_path(external_function);

    log("GET DECISIONS")

    sqlquery=`
        INSERT INTO `+CONCORDANCE_INTERFACE_TABLE+`
                (request_type,name,country,state,website,task_id, task_index,status,map_status,entity_id, concordance )
            WITH tasks AS (
                SELECT * 
                FROM (
                    SELECT request_type, name, country, state,website,task_id,task_index, status
                    FROM  `+CONCORDANCE_INTERFACE_TABLE+` 
                    QUALIFY 1=(row_number() over (partition by task_id, task_index order by create_ts desc)))
                WHERE status = '`+STATUS_PENDING+`'
            )
            SELECT '`+REQUEST_TYPE_DECISION+`' request_type
                ,concordance:"name"::varchar requested_name
                ,concordance:"country"::varchar requested_country
                ,concordance:"state"::varchar requested_state
                ,concordance:"url"::varchar requested_url
                ,concordance:"taskId"::varchar task_id
                ,concordance:"rowIndex"::int task_index
                ,case when (concordance:"response"[0]."mapStatus"::varchar) is null then '`+STATUS_PENDING+`'
                        when (concordance:"response"[0]."mapStatus"::varchar)='`+STATUS_MAPPED+`' then '`+STATUS_COMPLETED+`' 
                        else '`+STATUS_REVIEW+`' end  status
                ,concordance:"response"[0]."mapStatus"::varchar map_status
                ,concordance:"response"[0]."entityId"::varchar entity_id     
                ,concordance
            FROM (  
                SELECT `+FULLY_QUALIFIED_PATH+`(name, country, state,website,task_id,task_index)[0] concordance
                FROM tasks 
                ORDER BY task_id, task_index
            )
    `;
    snowflake.execute({sqlText: sqlquery});

    sqlquery=`
        MERGE INTO `+CONCORDANCE_TABLE+` t
            USING ( 
              SELECT name,country,state,website, status, map_status, entity_id
              FROM (
                SELECT name,country,state,website, status, map_status, entity_id, task_id, task_index
                FROM `+CONCORDANCE_INTERFACE_TABLE_STREAM+`
                WHERE request_type='`+REQUEST_TYPE_DECISION+`' 
                    AND METADATA$ACTION='INSERT')
              QUALIFY 1=ROW_NUMBER() OVER (PARTITION BY name,country,state,website ORDER BY status, map_status, entity_id)) s
            ON t.name=s.name 
                AND nvl(t.country,'')=nvl(s.country,'') 
                AND nvl(t.state,'')=nvl(s.state,'') 
                AND nvl(t.website,'')=nvl(s.website,'') 
                AND nvl(t.name,'')=nvl(s.name,'')
            WHEN MATCHED 
                THEN UPDATE SET t.map_status=s.map_status, t.entity_id = s.entity_id
                                ,t.status=s.status, last_modified_ts=current_timestamp()
            WHEN NOT MATCHED 
                THEN INSERT (name,country,state,website, status, map_status , entity_id) 
                    VALUES (s.name,s.country,s.state,s.website, s.status, s.map_status, s.entity_id )
    `;
    snowflake.execute({sqlText: sqlquery});
}

// -----------------------------------------------------------------------------
// main function with dispatcher logic. Call main method function based
//   on method requested via input parameter
// -----------------------------------------------------------------------------
try {

    init();

    log("procName: " + procName + " " + STATUS_BEGIN);
    flush_log(STATUS_BEGIN);

    if (METHOD==METHOD_CONF) {
        concordance_configure(CONCORDANCE_INTERFACE_TABLE);
    } else if (METHOD==METHOD_POST){
        concordance_task_post(EXTERNAL_FUNCTION)
    } else if (METHOD==METHOD_GET) {
        concordance_task_get(EXTERNAL_FUNCTION)
    } else {
        throw new Error("REQUESTED METHOD NOT FOUND: "+METHOD+"; ALLOWED VALUES "+METHOD_CONF+","+METHOD_POST+","+METHOD_GET);
    }

    log("procName: " + procName + " " + STATUS_END);
    flush_log(STATUS_END);
    return return_array; 
}
catch (err) {
    log("ERROR found - MAIN try command");
    log("err.code: " + err.code);
    log("err.state: " + err.state);
    log("err.message: " + err.message);

    flush_log(STATUS_FAILURE);

    return return_array;
}
$$;

