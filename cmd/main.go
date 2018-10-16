package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	file, err := os.Open("/Users/chrisachilli/creditsuisse/bigsample.log")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	db, err := sql.Open("mysql",
		"root:password@tcp(127.0.0.1:3306)/test")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS LOG_MONITOR ( id varchar(10), duration bigint, type varchar(50), host varchar(20), alert varchar(10))")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("TRUNCATE TABLE LOG_MONITOR")

	stmt, err := db.Prepare("INSERT INTO LOG_MONITOR values (?,?,?,?,?)")
	if err != nil {
		log.Fatal(err)
	}

	type LogLine struct {
		ID        string  `json:"id"`
		State     string  `json:"state"`
		Type      string  `json:"type"`
		Host      string  `json:"host"`
		Timestamp float64 `json:"timestamp"`
	}

	type DbRecord struct {
		ID       string  `json:"id"`
		Duration float64 `json:"duration"`
		Type     string  `json:"type"`
		Host     string  `json:"host"`
		Alert    string  `json:"alert"`
	}

	counter := 0
	scanner := bufio.NewScanner(file)

	tempMap := make(map[string]LogLine)
	alert := "false"

	started := time.Now().UnixNano()

	for scanner.Scan() {
		counter++
		if counter%10000 == 0 {
			fmt.Printf("\r %s %d", "So far analysed: ", counter)
		}
		myjson := scanner.Text()
		//fmt.Println("MYJSON: ", myjson)
		var logline LogLine
		err = json.Unmarshal([]byte(myjson), &logline)
		//fmt.Println("logline: ", &logline)

		tempLog, ok := tempMap[logline.ID]
		if ok {
			duration := math.Abs(tempLog.Timestamp - logline.Timestamp)
			if duration >= 4 {
				alert = "true"
			} else {
				alert = "false"
			}
			dbRecord := DbRecord{logline.ID, duration, logline.Type, logline.Host, alert}
			//fmt.Println(dbRecord)
			delete(tempMap, logline.ID)
			_, err := stmt.Exec(dbRecord.ID, dbRecord.Duration, dbRecord.Type, dbRecord.Host, dbRecord.Alert)
			if err != nil {
				log.Fatal(err)
			}

		} else {
			tempMap[logline.ID] = logline
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\r\n")
	fmt.Println("Done in ", (time.Now().UnixNano()-started)/100000000, " seconds!")

}
