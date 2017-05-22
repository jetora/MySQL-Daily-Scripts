package main

import (
    "database/sql"
    "flag"
    "fmt"
    _ "github.com/go-sql-driver/mysql"
    "log"
    "strconv"
    "strings"
    "sort"
)

var myDB *sql.DB

type Hostinfo struct {
    DBUser,
    DBPassword,
    DBname,
    DBHost,
    DBPort,
    DBChar string
}

func connMysql(host *Hostinfo) (*sql.DB, error) {
    if host.DBHost != "" {
        host.DBHost = "tcp(" + host.DBHost + ":" + host.DBPort + ")"
    }
    db, err := sql.Open("mysql", host.DBUser+":"+host.DBPassword+"@"+host.DBHost+"/"+host.DBname+"?charset="+host.DBChar)
    return db, err
}
func SetDB(ip string) (myDB *sql.DB) {
    var server_info Hostinfo
    server_info.DBUser = "xxx"
    server_info.DBPassword = "xxx"
    server_info.DBname = "test"
    server_info.DBHost = ip
    server_info.DBPort = "3358"
    server_info.DBChar = "utf8"
    myDB, _ = connMysql(&server_info)
    return myDB
}
func handleError(err error) {
    if err != nil {
        log.Fatal(err)
    }
}
func get_data_arr(ip, tmp_sql string) []string {
    myDB = SetDB(ip)
    defer myDB.Close()
    rows, err := myDB.Query(tmp_sql)
    defer rows.Close()
    handleError(err)
    columns, err := rows.Columns()
    handleError(err)
    values := make([]sql.RawBytes, len(columns))
    scanArgs := make([]interface{}, len(values))
    for i := range values {
        scanArgs[i] = &values[i]
    }
    var tmpstr []string
    for rows.Next() {
        err = rows.Scan(scanArgs...)
        handleError(err)
        var value string
        for _, col := range values {
            if col == nil {
                value = "NULL"
            } else {
                value = string(col)
            }
            tmpstr = append(tmpstr, value)
        }
    }
    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
    return tmpstr
}
func get_data_map(ip, tmp_sql string) []map[string]string {
    myDB = SetDB(ip)
    defer myDB.Close()
    rows, err := myDB.Query(tmp_sql)
    defer rows.Close()
    handleError(err)
    columns, err := rows.Columns()
    handleError(err)
    values := make([]sql.RawBytes, len(columns))
    scanArgs := make([]interface{}, len(values))
    for i := range values {
        scanArgs[i] = &values[i]
    }
    var result []map[string]string

    for rows.Next() {
        err = rows.Scan(scanArgs...)
        handleError(err)
        each := make(map[string]string)

        for i, col := range values {
            each[columns[i]] = string(col)
        }
        result = append(result, each)
    }
    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
    return result
}
func get_master(ip string) string {
    slave_stats_sql := "show slave status"
    iptmp := ip
    arr_tmp := get_data_map(iptmp, slave_stats_sql)
    if len(arr_tmp) == 0 {
        return iptmp
    }
    if arr_tmp[0]["Master_Host"] == "1.1.1.1" {
        return iptmp
    }
    return get_master(arr_tmp[0]["Master_Host"])
}

var results []string
var m_ip string

func get_slave(ip string) []string {
    slave_ip_sql := "select SUBSTRING_INDEX(host,':',1) from information_schema.processlist where user='replicater' and command='Binlog Dump'"
    slave_ip_arr := get_data_arr(ip, slave_ip_sql)
    for _, ip_value := range slave_ip_arr {
        if ip == m_ip {
            results = append(results, m_ip+":"+ip_value)
        } else {
            results = append(results, m_ip+":"+ip+":"+ip_value)
        }
        if len(get_slave(ip_value)) == 0 {
            continue
        }
    }
    return results
}

func get_binlog(mip string, each map[string]string) map[string]string {
    master_status := "show master status"
    slave_status := "show slave status"
    slave_ip_sql := "select SUBSTRING_INDEX(host,':',1) from information_schema.processlist where user='replicater' and command='Binlog Dump'"

    masterbinlog := get_data_map(mip, master_status)[0]["File"]
    //each[ip] = masterbinlog
    master_min_binlog := masterbinlog
    child_level_arr := get_data_arr(mip, slave_ip_sql)
    for _, ipvalue := range child_level_arr {
        child_level_binlog := get_data_map(ipvalue, slave_status)[0]["Master_Log_File"]
        if child_level_binlog < master_min_binlog {
            master_min_binlog = child_level_binlog
        }
        grand_child_level_arr := get_data_arr(ipvalue, slave_ip_sql)
        if len(grand_child_level_arr) == 0 {
            each[ipvalue] = get_data_map(ipvalue, master_status)[0]["File"]
        } else {
            get_binlog(ipvalue, each)
        }

    }
    each[mip] = master_min_binlog
    return each
}

func purge_logs(map_arr map[string]string, ip string) {
    for k, v := range map_arr {
        //fmt.Println(k, ":", v, "  ")
        if k == ip {
            var ma_sep_group_arr []string
            if get_version(k) == "5" {
                ma_sep_group_arr = get_low_ver_master_logs(k,v)
            } else {
                ma_sep_group_arr = rsync_master_logs(k, v)
            }

            for _, value := range ma_sep_group_arr {
                fmt.Println(k, value)
                myDB = SetDB(k)
                defer myDB.Close()
                myDB.Exec("purge master logs to '" + value + "'")
            }
        } else {
            myDB = SetDB(k)
            defer myDB.Close()
            fmt.Println(k, v)
            myDB.Exec("purge master logs to '" + v + "'")
        }
    }
}

func fill_str(str string) string {
    zero_str := ""
    max_len := 6
    len_str := len(str)
    if len_str < max_len {
        repeat_count := (max_len - len_str)
        for i := 0; i < repeat_count; i++ {
            zero_str += "0"
        }
    }
    str = zero_str + str
    return str
}

func get_version(ip string) string {
    version_sql := "select @@version"
    version := strings.Split(get_data_arr(ip, version_sql)[0], ".")[1]
    return version
}

func rsync_master_logs(ip, m_binlog string) []string {
    batch := 10
    var ma_all_arr []string
    m_binlog_num := strings.Split(m_binlog, ".")[1]
    log_sql := "select reverse(substring_index(reverse(substring_index(file_name,'/',6)),'/',1)) from performance_schema.file_instances where file_name REGEXP 'mysql-bin.[0-9]{1,2}' and reverse(substring_index(reverse(substring_index(file_name,'/',6)),'/',1)) <= " + "'" + m_binlog + "'"
    binlog_arr := get_data_arr(ip, log_sql)
    repeat_count := len(binlog_arr) / batch
    m_log_int, err := strconv.Atoi(m_binlog_num)
    handleError(err)
    if len(binlog_arr)%batch == 0 {
        for i := repeat_count - 1; i >= 0; i-- {
            log_str := strconv.Itoa(m_log_int - i*batch)
            fmt.Println(fill_str(log_str))
            ma_all_arr = append(ma_all_arr, "mysql-bin."+fill_str(log_str))
        }
    } else {
        for i := repeat_count; i >= 0; i-- {
            log_str := strconv.Itoa(m_log_int - i*batch)
            //fmt.Println(fill_str(log_str))
            ma_all_arr = append(ma_all_arr, "mysql-bin."+fill_str(log_str))
        }
    }
    return ma_all_arr
}

func get_equal(arr []string,m_binlog string) int {
    equal_k := 0
    for k,v :=range arr{
        if v == m_binlog{
            equal_k = k
            break
        }
    } 
    return equal_k 
}

func get_low_ver_master_logs(ip,m_binlog string) []string {
    batch := 10
    var ma_all_arr []string
    binlog_arr_map := get_data_map(ip, "show binary logs")
    var log_arr []string 
    for _,value:=range binlog_arr_map{
        log_arr=append(log_arr,value["Log_name"])
    }
    sort.Strings(log_arr)
    kk:=get_equal(log_arr,m_binlog)
    n_arr := log_arr[:kk+1]
    repeat_count := len(n_arr) / batch
    m_binlog_num := strings.Split(m_binlog, ".")[1]
    m_log_int, err := strconv.Atoi(m_binlog_num)
    handleError(err)
    if len(n_arr)%batch == 0 {
        for i := repeat_count - 1; i >= 0; i-- {
            log_str := strconv.Itoa(m_log_int - i*batch)
            ma_all_arr = append(ma_all_arr, "mysql-bin."+fill_str(log_str))
        }
    }else {
        for i := repeat_count; i >= 0; i-- {
            log_str := strconv.Itoa(m_log_int - i*batch)
            ma_all_arr = append(ma_all_arr, "mysql-bin."+fill_str(log_str))
        }
    }
    return ma_all_arr
}

func main() {
    var ip string
    flag.StringVar(&ip, "n", "127.0.0.1", "which ip(purge master logs for all nodes),note: if master's binlog count large than 10,it will be batch purged...")
    flag.Parse()
    m_ip = get_master(ip)
    each := make(map[string]string)
    ip_log_map := get_binlog(m_ip, each)
    purge_logs(ip_log_map, m_ip)
}

