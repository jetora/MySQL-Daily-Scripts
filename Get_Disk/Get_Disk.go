package main

import (
    "database/sql"
    "flag"
    "fmt"
    _ "github.com/go-sql-driver/mysql"
    "log"
    "strconv"
    "./ssh"
    "strings"
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
func get_data(ip, tmp_sql string) []string {
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
func get_master(ip string) string {
    slave_stats_sql := "show slave status"
    iptmp := ip
    slave_stats_arr := get_data(iptmp, slave_stats_sql)
    if len(slave_stats_arr) == 0 {
        return iptmp
    }
    if slave_stats_arr[1] == "1.1.1.1" {
        return iptmp
    }
    return get_master(slave_stats_arr[1])
}

var results []string
var m_ip string
/*
func get_slave(ip string) []string {
    slave_ip_sql := "select SUBSTRING_INDEX(host,':',1) from information_schema.processlist where user='replicater' and command='Binlog Dump'"
    slave_ip_arr := get_data(ip, slave_ip_sql)
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
}*/

func get_slave(ipstr string) []string {
	slave_ip_sql := "select SUBSTRING_INDEX(host,':',1) from information_schema.processlist where user='replicater' and command='Binlog Dump'"
	var slave_ip_arr []string
	if len(strings.Split(ipstr, ":")) == 1 {
		slave_ip_arr = get_data(ipstr, slave_ip_sql)
	} else {
		slave_ip_arr = get_data(strings.Split(ipstr, ":")[len(strings.Split(ipstr, ":"))-1], slave_ip_sql)
	}

	for _, ip_value := range slave_ip_arr {
		if len(strings.Split(ipstr, ":")) == 1 {
			results = append(results, m_ip+":"+ip_value)
		} else {
			results = append(results, ipstr+":"+ip_value)
		}
		if len(get_slave(ipstr+":"+ip_value)) == 0 {
			continue
		}
	}
	return results
}

func to_btree() {
    for _, value := range results {
        var do_str string = "  "
        if len(strings.Split(value, ":")) == 1 {
            fmt.Println("Master:" + value)
            ssh.Ssh(value,"df -h|grep -E 'Size|export'")
        } else if len(strings.Split(value, ":")) > 1 {
            for t_len := 0; t_len < len(strings.Split(value, ":")); t_len++ {
                do_str += do_str
            }
            fmt.Println("Level" + strconv.Itoa(len(strings.Split(value, ":"))) + ":" + do_str + strings.Split(value, ":")[len(strings.Split(value, ":"))-1])
            ssh.Ssh(strings.Split(value, ":")[len(strings.Split(value, ":"))-1],"df -h|grep -E 'Size|export'")
        }
    }
/*
    fmt.Println("####################disk info####################")
    for _, value := range results {
        if len(strings.Split(value, ":")) == 1 {
            ssh.Ssh(value)
        } else if len(strings.Split(value, ":")) == 2 {
            ssh.Ssh(strings.Split(value, ":")[1])
        } else if len(strings.Split(value, ":")) == 3 {
            ssh.Ssh(strings.Split(value, ":")[2])
        }
    }
*/
}
func main() {
    var ip string
    flag.StringVar(&ip, "n", "127.0.0.1", "which ip")
    flag.Parse()
    m_ip = get_master(ip)
    results = append(results, m_ip)
    get_slave(m_ip)
    to_btree()

}

