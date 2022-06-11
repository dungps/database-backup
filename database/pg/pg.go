package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/umisama/go-sqlbuilder"
	"github.com/umisama/go-sqlbuilder/dialects"
	database2 "react-web-backup/database"
	"react-web-backup/utils"
	"reflect"
	"strings"

	_ "github.com/lib/pq"
)

func init() {
	database2.RegisterDb(&DB{})
}

type ColumnInfo struct {
	OrdinalPosition   int64  `json:"ordinal_position,omitempty"`
	ColumnName        string `json:"column_name,omitempty"`
	DataType          string `json:"data_type,omitempty"`
	FormatType        string `json:"format_type,omitempty"`
	NumericPrecision  int    `json:"numeric_precision,omitempty"`
	DatetimePrecision string `json:"datetime_precision,omitempty"`
	NumericScale      int    `json:"numeric_scale,omitempty"`
	DataLength        int    `json:"data_length,omitempty"`
	IsNullable        string `json:"is_nullable,omitempty"`
	Check             string `json:"check,omitempty"`
	CheckConstraint   string `json:"check_constraint,omitempty"`
	ColumnDefault     string `json:"column_default,omitempty"`
	ForeignKey        string `json:"foreign_key,omitempty"`
	Comment           string `json:"comment,omitempty"`
}

type DB struct {
	config *database2.Connection
	conn   *sql.DB
	c2Name map[string]string
	c2Kind map[string]reflect.Kind
	c2Type map[string]reflect.Type
	values []interface{}
}

func (d *DB) Name() string {
	return "pg"
}

func (d *DB) Connect(ctx context.Context, c *database2.Connection) error {
	conn, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			c.Host,
			c.Port,
			c.Username,
			c.Password,
			c.Name,
		),
	)
	if err != nil {
		return err
	}

	if err := conn.PingContext(ctx); err != nil {
		return err
	}

	d.config = c
	d.conn = conn

	return d.init()
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
		sqlString += fmt.Sprintf("%s\n\n", s)
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
	qb := squirrel.
		Select("table_name").
		From("information_schema.tables").
		PlaceholderFormat(squirrel.Dollar).
		Where(squirrel.Eq{"table_catalog": d.config.Name}).
		Where(squirrel.Eq{"table_schema": d.config.Schema})

	query, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := d.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
	qb := squirrel.
		Select(d.getSelectColumns()...).
		From("information_schema.columns").
		PlaceholderFormat(squirrel.Dollar).
		Where(squirrel.Eq{"table_name": name}).
		Where(squirrel.Eq{"table_schema": d.config.Schema}).
		Join("pg_attribute pa ON attrelid=24578 AND attname=column_name")

	query, args, err := qb.ToSql()
	if err != nil {
		return "", err
	}

	rows, err := d.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cs, err := rows.Columns()
	if err != nil {
		return "", err
	}

	columns := make(map[string]*ColumnInfo)
	for rows.Next() {
		dest := &ColumnInfo{}
		ve := reflect.ValueOf(dest).Elem()
		values := d.values
		err := rows.Scan(values...)
		if err != nil {
			return "", err
		}

		for i, c := range cs {
			fieldName := d.c2Name[c]
			kind, _ := d.c2Kind[c]
			value := values[i]

			switch kind {
			default:
				ve.FieldByName(fieldName).SetString(value.(*sql.NullString).String)
			case reflect.Bool:
				ve.FieldByName(fieldName).SetBool(value.(*sql.NullBool).Bool)
			case reflect.Float32, reflect.Float64:
				ve.FieldByName(fieldName).SetFloat(value.(*sql.NullFloat64).Float64)
			case reflect.Int64, reflect.Int32, reflect.Int, reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint32, reflect.Uint16:
				ve.FieldByName(fieldName).SetInt(value.(*sql.NullInt64).Int64)
			}
		}

		columns[dest.ColumnName] = dest
	}

	return d.buildCreateTable(name, columns)
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
			default:
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

func (d *DB) getSelectColumns() []string {
	return []string{
		"information_schema.columns.ordinal_position as ordinal_position",
		"information_schema.columns.column_name as column_name",
		"information_schema.columns.udt_name AS data_type",
		"format_type(atttypid, atttypmod) as format_type",
		"information_schema.columns.numeric_precision as numeric_precision",
		"information_schema.columns.datetime_precision as datetime_precision",
		"information_schema.columns.numeric_scale as numeric_scale",
		"information_schema.columns.character_maximum_length AS data_length",
		"information_schema.columns.is_nullable as is_nullable",
		"information_schema.columns.column_name as check",
		"information_schema.columns.column_name as check_constraint",
		"information_schema.columns.column_default as column_default",
		"information_schema.columns.column_name AS foreign_key",
		"pg_catalog.col_description(24578,ordinal_position) as comment",
	}
}

func (d *DB) init() error {
	d.c2Type = make(map[string]reflect.Type)
	d.c2Kind = make(map[string]reflect.Kind)
	d.c2Name = make(map[string]string)
	d.values = make([]interface{}, 0)
	te := reflect.TypeOf(&ColumnInfo{}).Elem()
	ve := reflect.ValueOf(&ColumnInfo{}).Elem()
	for i := 0; i < te.NumField(); i++ {
		f := te.Field(i)
		dbKey := getKeyFromTag(f.Tag.Get("json"))
		if dbKey == "-" || len(dbKey) == 0 {
			continue
		}
		d.c2Name[dbKey] = f.Name
		d.c2Kind[dbKey] = ve.FieldByName(f.Name).Kind()
		d.c2Type[dbKey] = ve.FieldByName(f.Name).Type()

		switch ve.FieldByName(f.Name).Kind() {
		default:
			d.values = append(d.values, &sql.NullString{})
		case reflect.Bool:
			d.values = append(d.values, &sql.NullBool{})
		case reflect.Float32, reflect.Float64:
			d.values = append(d.values, &sql.NullFloat64{})
		case reflect.Int64, reflect.Int32, reflect.Int, reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint32, reflect.Uint16:
			d.values = append(d.values, &sql.NullInt64{})
		}
	}

	return nil
}

func (d *DB) buildCreateTable(name string, dest map[string]*ColumnInfo) (string, error) {
	sqlbuilder.SetDialect(dialects.Postgresql{})

	columns := make([]sqlbuilder.ColumnConfig, 0)

	for cn, c := range dest {
		columns = append(columns, d.buildColumn(cn, c))
	}

	table := sqlbuilder.NewTable(
		name,
		&sqlbuilder.TableOption{},
		columns...,
	)

	query, _, err := sqlbuilder.CreateTable(table).ToSql()
	if err != nil {
		return "", err
	}

	return query, nil
}

func (d *DB) buildColumn(name string, info *ColumnInfo) sqlbuilder.ColumnConfig {
	o := &sqlbuilder.ColumnOption{
		NotNull: info.IsNullable == "No",
		SqlType: info.DataType,
	}

	if len(info.ColumnDefault) > 0 {
		o.Default = info.ColumnDefault
	}

	return sqlbuilder.AnyColumn(name, o)
}

func getKeyFromTag(tag string) string {
	pieces := utils.StringSlice(tag, ",")
	if len(pieces) == 0 {
		return ""
	}
	return pieces[0]
}
