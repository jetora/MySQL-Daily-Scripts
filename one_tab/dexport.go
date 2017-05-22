package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	_ "mysql"
	"os"
	"path"
	"strings"
	"xlsx"
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
func SetDB(db_info string) (myDB *sql.DB) {
	dbstr := strings.Split(db_info, ":")
	var server_info Hostinfo
	server_info.DBUser = "xxx"
	server_info.DBPassword = "xxx"
	server_info.DBname = dbstr[1]
	server_info.DBHost = dbstr[0]
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
func w_excel(tsql string, output_file string, db_info string) {
	var xlsxFile *xlsx.File
	var sheet *xlsx.Sheet
	xlsxFile = xlsx.NewFile()
	sheet, _ = xlsxFile.AddSheet("Sheet1")
	style := xlsx.NewStyle()
	fill := *xlsx.NewFill("solid", "FFD9D9D9", "FFD9D9D9")
	font := *xlsx.NewFont(9, "Verdana")
	border := *xlsx.NewBorder("thin", "thin", "thin", "thin")
	style.Fill = fill
	style.Font = font
	style.Border = border
	style.ApplyFill = true
	style.ApplyFont = true
	style.ApplyBorder = true
	col_style := xlsx.NewStyle()
	col_font := *xlsx.NewFont(9, "Verdana")
	col_border := *xlsx.NewBorder("thin", "thin", "thin", "thin")
	col_style.Font = col_font
	col_style.Border = col_border
	col_style.ApplyFont = true
	col_style.ApplyBorder = true
	myDB = SetDB(db_info)
	defer myDB.Close()
	rows, err := myDB.Query(tsql)
	defer rows.Close()
	handleError(err)
	columns, err := rows.Columns()
	handleError(err)
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	row := sheet.AddRow()
	for i := 0; i < len(columns); i++ {
		cell := row.AddCell()
		cell.Value = columns[i]
		cell.SetStyle(style)
		handleError(err)
	}
	count := 0
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		handleError(err)
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
			cell.SetStyle(col_style)
		}
		count++
	}
	if err = rows.Err(); err != nil {
		fmt.Println(err.Error())
		return
	}
	err = xlsxFile.Save(output_file)
	handleError(err)
	fmt.Printf("%d rows exported...\n", count)
}
func read_file(input_file string) string {
	var tstr string
	fi, err := os.Open(input_file)
	handleError(err)
	defer fi.Close()
	br := bufio.NewReader(fi)
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		tstr += (string(a)+" ")
	}
	return tstr
}
func TarGz(srcDirPath string, destFilePath string) {
	fw, err := os.Create(destFilePath)
	handleError(err)
	defer fw.Close()
	// Gzip writer
	gw := gzip.NewWriter(fw)
	defer gw.Close()
	// Tar writer
	tw := tar.NewWriter(gw)
	defer tw.Close()
	// Check if it's a file or a directory
	f, err := os.Open(srcDirPath)
	handleError(err)
	fi, err := f.Stat()
	handleError(err)
	if fi.IsDir() {
		// handle source directory
		fmt.Println("Cerating tar.gz from directory...")
		tarGzDir(srcDirPath, path.Base(srcDirPath), tw)
	} else {
		// handle file directly
		tarGzFile(srcDirPath, fi.Name(), tw, fi)
	}
	fmt.Println("Export Done")
}
func tarGzDir(srcDirPath string, recPath string, tw *tar.Writer) {
	// Open source diretory
	dir, err := os.Open(srcDirPath)
	handleError(err)
	defer dir.Close()

	// Get file info slice
	fis, err := dir.Readdir(0)
	handleError(err)
	for _, fi := range fis {
		// Append path
		curPath := srcDirPath + "/" + fi.Name()
		// Check it is directory or file
		if fi.IsDir() {
			// Directory
			// (Directory won't add unitl all subfiles are added)
			fmt.Printf("Adding path...%s\\n", curPath)
			tarGzDir(curPath, recPath+"/"+fi.Name(), tw)
		} else {
			// File
			fmt.Printf("Adding file...%s\\n", curPath)
		}

		tarGzFile(curPath, recPath+"/"+fi.Name(), tw, fi)
	}
}

// Deal with files
func tarGzFile(srcFile string, recPath string, tw *tar.Writer, fi os.FileInfo) {
	if fi.IsDir() {
		// Create tar header
		hdr := new(tar.Header)
		// if last character of header name is '/' it also can be directory
		// but if you don't set Typeflag, error will occur when you untargz
		hdr.Name = recPath + "/"
		hdr.Typeflag = tar.TypeDir
		hdr.Size = 0
		//hdr.Mode = 0755 | c_ISDIR
		hdr.Mode = int64(fi.Mode())
		hdr.ModTime = fi.ModTime()

		// Write hander
		err := tw.WriteHeader(hdr)
		handleError(err)
	} else {
		// File reader
		fr, err := os.Open(srcFile)
		handleError(err)
		defer fr.Close()

		// Create tar header
		hdr := new(tar.Header)
		hdr.Name = recPath
		hdr.Size = fi.Size()
		hdr.Mode = int64(fi.Mode())
		hdr.ModTime = fi.ModTime()

		// Write hander
		err = tw.WriteHeader(hdr)
		handleError(err)

		// Write file data
		_, err = io.Copy(tw, fr)
		handleError(err)
	}
}
func main() {
	var dbinfo string
	var output_file string
	flag.StringVar(&dbinfo, "dbinfo", "IP:DB", "dbinfo")
	flag.StringVar(&output_file, "output_file", "results", "output_file")
	flag.Parse()
	w_excel(read_file("sql.txt"), "./results/"+output_file+dbinfo+".xlsx", dbinfo)
	targetFilePath := "./results/" + output_file+dbinfo+ ".tar.gz"
	srcDirPath := "./results/" + output_file+dbinfo+ ".xlsx"
	TarGz(srcDirPath, targetFilePath)
	os.Remove("./results/" + output_file+dbinfo+ ".xlsx")
}
