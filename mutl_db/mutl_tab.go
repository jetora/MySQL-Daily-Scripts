package main

import (
    "runtime"
	"database/sql"
	"fmt"
    "os"
	_ "mysql"
	"xlsx"
)

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
	server_info.DBUser = "xxxx"
	server_info.DBPassword = "xxxx"
	server_info.DBname = "test"
	server_info.DBHost = ip
	server_info.DBPort = "3358"
	server_info.DBChar = "utf8"
	tmyDB, _ := connMysql(&server_info)
	return tmyDB
}
func get_tab(ip string) []string {
	tmp_sql := "select concat(TABLE_SCHEMA,'.',TABLE_NAME) from information_schema.TABLES where TABLE_SCHEMA like 'jdtreasure%' and TABLE_NAME='treasure_item_vender'"
	var tabstr []string
	myDB := SetDB(ip)
	defer myDB.Close()
	rows, err := myDB.Query(tmp_sql)
	defer rows.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
	columns, err := rows.Columns()
	if err != nil {
		fmt.Println(err.Error())
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			fmt.Println(err.Error())
		}
		var value string
		for _, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
				tabstr = append(tabstr, string(value))
			}
		}
	}
	if err = rows.Err(); err != nil {
		fmt.Println(err.Error())
	}
	return tabstr
}
func w_excel(output_file string, ip string) {
	var xlsxFile *xlsx.File
	var sheet *xlsx.Sheet
	xlsxFile = xlsx.NewFile()
	sheet, _ = xlsxFile.AddSheet("Sheet1")
	tab_arr := get_tab(ip)
	for _,tab := range tab_arr {
        tsql := "select  id, item_id, vender_id, product_sku, stock, opened_issue_amount, agent_level, status,item_type, data_channel,created from "+tab+" where yn =1  order by  vender_id desc"
        fmt.Println(tsql)
		myDB := SetDB(ip)
		defer myDB.Close()
		rows, err := myDB.Query(tsql)
		defer rows.Close()
		if rows == nil {
			fmt.Println(err.Error())
			continue
		}
		columns, _ := rows.Columns()
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			row := sheet.AddRow()
			var value string
			for _, col := range values {
				if col == nil {
					value = "NULL"
				} else {
					value = string(col)
				}
				cell := row.AddCell()
				cell.Value = value
			}
		}
		if err := rows.Err(); err != nil {
			fmt.Println(err.Error())
			return
		}
        err = xlsxFile.Save(output_file)
                if err != nil {
                    fmt.Println(err.Error())
                    return
                }
	}
}
func cal_ex(ip, output_file, d_file string) {
    xlFile, _ := xlsx.OpenFile(output_file)
    w_xlsxFile, _ := xlsx.OpenFile(d_file)
    w_sheet := w_xlsxFile.Sheets[0]
    sheet := xlFile.Sheets[0]
    var err error
    var w_row *xlsx.Row
    for _, row := range sheet.Rows {
        w_row = w_sheet.AddRow()
        for _, cell := range row.Cells {
            value, _ := cell.String()
            w_cell := w_row.AddCell()
            w_cell.Value = value
        }
    }
    err = w_xlsxFile.Save(d_file)
    if err != nil {
        fmt.Println(err.Error())
        return
    }
}
func main() {
    ips := []string{"1.1.1.1","2.2.2.2"}
    runtime.GOMAXPROCS(runtime.NumCPU()*2)
    output_file := "jdorders"
    d_file:="results.xlsx"
    const (
        GOROUTINE_COUNT = 8 
        TASK_COUNT      = 8
    )
    chReq := make(chan string, GOROUTINE_COUNT)
    chRes := make(chan int, GOROUTINE_COUNT)
    for i := 0; i < GOROUTINE_COUNT; i++ {
        go func() {
            for {
                url := <-chReq
                w_excel(output_file+url+".xlsx", url)
                chRes <- 0
            }
        }()
    }
    go func() {
        urls := make([]string, TASK_COUNT)
        for i := 0; i < TASK_COUNT; i++ {
            urls[i] = ips[i]
        }
        for i := 0; i < TASK_COUNT; i++ {
            chReq <- urls[i]
        }
    }()
    for i := 0; i < TASK_COUNT; i++ {
        d := <-chRes
        _ = d
    }
    var xlsxFile *xlsx.File
    xlsxFile = xlsx.NewFile()
    xlsxFile.AddSheet("Sheet1")
    err := xlsxFile.Save(d_file)
    if err != nil {
         fmt.Println(err.Error())
         return
    }
    for i,_:=range ips{
        cal_ex(ips[i], output_file+ips[i]+".xlsx", d_file)
    }
    for i,_:=range ips{
        os.Remove(output_file+ips[i]+".xlsx")
    }
}
