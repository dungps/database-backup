package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	database2 "react-web-backup/database"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	database2.RegisterDb(&DB{})
}

type DB struct {
	conn *sql.DB
}

func (d *DB) Name() string {
	return "mysql"
}

func (d *DB) Connect(ctx context.Context, c *database2.Connection) error {
	conn, err := sql.Open(
		d.Name(),
		fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s",
			c.Username,
			c.Password,
			c.Host,
			c.Port,
			c.Name,
		),
	)
	if err != nil {
		return err
	}

	if err := conn.PingContext(ctx); err != nil {
		return err
	}

	d.conn = conn
	return nil
}

func (d *DB) Backup(ctx context.Context) (string, error) {
	tables, err := d.getTables(ctx)
	if err != nil {
		return "", err
	}

	sqlString := ""

	// backup create tables
	for _, t := range tables {
		s, err := d.getCreateTableQuery(ctx, t)
		if err != nil {
			return "", err
		}
		sqlString += fmt.Sprintf("%s;\n\n", s)
	}

	for _, t := range tables {
		insertSql, err := d.getTableData(ctx, t)
		if err != nil {
			return "", err
		}
		sqlString += fmt.Sprintf("%s\n\n", insertSql)
	}

	// backup data

	return sqlString, nil
}

func (d *DB) Restore(ctx context.Context, fileContent string) error {
	_, err := d.conn.ExecContext(ctx, fileContent)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) getTables(ctx context.Context) ([]string, error) {
	rows, err := d.conn.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	tables := make([]string, 0)

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, rows.Err()
}

func (d *DB) getCreateTableQuery(ctx context.Context, name string) (string, error) {
	rows, err := d.conn.QueryContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", name))
	if err != nil {
		return "", err
	}
	defer func() {
		_ = rows.Close()
	}()

	var table string
	var sql string

	for rows.Next() {
		err := rows.Scan(&table, &sql)
		if err != nil {
			return "", err
		}
		break
	}

	return sql, rows.Err()
}

func (d *DB) getTableData(ctx context.Context, name string) (string, error) {
	rows, err := d.conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", name))
	if err != nil {
		return "", err
	}
	defer func() {
		_ = rows.Close()
	}()

	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", errors.New("No columns in table " + name + ".")
	}

	columnsType, err := rows.ColumnTypes()
	if err != nil {
		return "", err
	}
	if len(columnsType) == 0 {
		return "", errors.New("No columns in table " + name + ".")
	}

	dataText := make([]string, 0)
	for rows.Next() {
		data := make([]interface{}, 0)
		for _, c := range columnsType {
			switch c.ScanType().Kind() {
			case reflect.String, reflect.Slice, reflect.Struct, reflect.Map, reflect.Ptr:
				data = append(data, &sql.NullString{})
			case reflect.Bool:
				data = append(data, &sql.NullBool{})
			case reflect.Float32, reflect.Float64:
				data = append(data, &sql.NullFloat64{})
			case reflect.Int64, reflect.Int32, reflect.Int, reflect.Uint64, reflect.Uint32, reflect.Uint:
				data = append(data, &sql.NullInt64{})
			}
		}

		if err := rows.Scan(data...); err != nil {
			return "", err
		}

		dataStrings := make([]string, 0)
		insertedColumns := make([]string, 0)

		for i, c := range columnsType {
			cType := c.ScanType().Kind()
			switch cType {
			case reflect.String, reflect.Slice, reflect.Struct, reflect.Map, reflect.Ptr:
				v := data[i].(*sql.NullString)
				if v != nil && v.Valid {
					dataStrings = append(dataStrings, fmt.Sprintf("'%s'", v.String))
					insertedColumns = append(insertedColumns, columns[i])
				}
			case reflect.Bool:
				v := data[i].(*sql.NullBool)
				if v != nil && v.Valid {
					var value = 1
					if !v.Bool {
						value = 0
					}
					dataStrings = append(dataStrings, fmt.Sprintf("%d", value))
					insertedColumns = append(insertedColumns, columns[i])
				}
			case reflect.Float32, reflect.Float64:
				v := data[i].(*sql.NullFloat64)
				if v != nil && v.Valid {
					dataStrings = append(dataStrings, fmt.Sprintf("%v", v.Float64))
					insertedColumns = append(insertedColumns, columns[i])
				}
			case reflect.Int64, reflect.Int32, reflect.Int, reflect.Uint64, reflect.Uint32, reflect.Uint:
				v := data[i].(*sql.NullInt64)
				if v != nil && v.Valid {
					dataStrings = append(dataStrings, fmt.Sprintf("%d", v.Int64))
					insertedColumns = append(insertedColumns, columns[i])
				}
			}
		}

		dataText = append(
			dataText,
			fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s);",
				name,
				strings.Join(insertedColumns, ", "),
				strings.Join(dataStrings, ","),
			),
		)
	}

	return strings.Join(dataText, "\n"), rows.Err()
}
