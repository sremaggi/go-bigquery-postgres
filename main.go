package main

import (
	bq "cloud.google.com/go/bigquery"
	"context"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	_ "gocloud.dev/postgres/gcppostgres"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

const (
	host          = "localhost"
	port          = 5432
	user          = "newuser"
	password      = "pasword"
	dbname        = "postgres"
	sslmode       = "disable"
	sslrootcert   = "./server-ca.pem"
	sslkey        = "./client-key.pem"
	sslcert       = "./client-cert.pem"
	projectID     = "txd-ia-money-mapping-dev"
	queryProducts = ``
)

var (
	count     = uint64(1)
	totalRows = uint64(0)
	//formatter dinamico para armar query
	i = int64(1)
)

type productRow struct {
	ProdID      bq.NullString `bigquery:"PROD_ID"`
	ProdEanID   bq.NullString `bigquery:"PROD_EAN_ID"`
	IDEstilo    bq.NullString `bigquery:"ID_ESTILO"`
	ProdDescTXT bq.NullString `bigquery:"PROD_DESC_TXT"`
}

func main() {

	log.Print("INIT POC BIGQUERY POSTGRES CLOUDSTORAGE")
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s sslrootcert=%s sslkey=%s sslcert=%s", host, port, user, password, dbname, sslmode, sslrootcert, sslkey, sslcert)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1000000)
	db.SetMaxIdleConns(1000000)
	db.SetConnMaxLifetime(5 * time.Minute)
	defer db.Close()
	createTable(db)
	ctx := context.Background()
	client, err := bq.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}

	defer client.Close()

	rows, err := query(ctx, client)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("EXTRACCION EXITOSA, LARGO", rows.TotalRows)
	err = iterRows(os.Stdout, rows, db)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("EXIT ERROR", err)
	//deleteDb(sqliteDatabase)

}

func query(ctx context.Context, client *bq.Client) (*bq.RowIterator, error) {
	log.Print("EXTRAYENDO ROWS DESDE BIGQUERY ...")
	query := client.Query(queryProducts)
	return query.Read(ctx)
}

//itera los resultados con el objeto RowsIterator
func iterRows(w io.Writer, iter *bq.RowIterator, db *sql.DB) error {
	wg := &sync.WaitGroup{}
	log.Print("TRANSFORMANDO ROWS EN ARCHIVO SQLLITE")

	const elementsN = 10000
	c := uint64(0)
	vals := []interface{}{}
	insertProductSql := `insert into products values `

	for {

		var row productRow
		err := iter.Next(&row)
		if iter.TotalRows == c {
			wg.Add(1)
			go insertDatos(db, iter.TotalRows, vals, insertProductSql, wg)
			wg.Wait()
			return nil
		}
		vals = append(vals, row.ProdID.String(), row.ProdEanID.String(), row.IDEstilo.String(), row.ProdDescTXT.String())
		insertProductSql += fmt.Sprintf("($%d,$%d,$%d,$%d),", i+0, i+1, i+2, i+3)

		if count == elementsN {
			totalRows += count
			wg.Add(1)
			go insertDatos(db, totalRows, vals, insertProductSql, wg)

			vals = []interface{}{}
			insertProductSql = `insert into products values `
			count = 0
			i = -3

		}

		count += 1
		i += 4
		c += 1

		if err != nil {
			return fmt.Errorf("error iterating through results: %v", err)
		}

	}
}

func insertDatos(db *sql.DB, totalRows uint64, elements []interface{}, query string, wg *sync.WaitGroup) {
	query = query[0 : len(query)-1]
	statement, err := db.Prepare(query)
	if err != nil {
		log.Fatalln(err.Error())
	}
	_, err = statement.Exec(elements...)
	if err != nil {
		log.Fatalln(err.Error())
	}
	log.Print("INSERT SUCCESFULL ", totalRows)
	wg.Done()
	statement.Close()
}
func createTable(db *sql.DB) {
	statement, err := db.Prepare(`
        CREATE TABLE products(
            PROD_ID TEXT,
            PROD_EAN_ID TEXT,
            ID_ESTILO TEXT,
            PROD_DESC_TXT TEXT
        );`)
	if err != nil {
		log.Fatal(err.Error())
	}
	statement.Exec()
	log.Println("Tabla creada")
}

func deleteDb(db *sql.DB) {
	deleteSQL := `a`
	statament, err := db.Prepare(deleteSQL)
	if err != nil {
		log.Fatal(err.Error())
	}
	_, err = statament.Exec()
	if err != nil {
		log.Fatal(err.Error())
	}
}
