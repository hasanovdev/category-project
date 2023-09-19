package postgres

import (
	"app/models"
	"app/pkg/helper"
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
)

type productRepo struct {
	db *pgxpool.Pool
}

func NewProductRepo(db *pgxpool.Pool) *productRepo {
	return &productRepo{
		db: db,
	}
}

func (r *productRepo) Create(req *models.CreateProduct) (string, error) {
	var (
		id    = uuid.New().String()
		query string
	)

	query = `
		INSERT INTO products(id, name, price,category_id, updated_at)
		VALUES ($1, $2, $3,$4,NOW())
	`

	_, err := r.db.Exec(context.Background(), query,
		id,
		req.Name,
		req.Price,
		req.Category_id,
	)

	if err != nil {
		return "", err
	}

	return id, nil
}

func (r *productRepo) GetByID(req *models.ProductPrimaryKey) (*models.Product, error) {
	var product models.Product
	query := `
		SELECT
			id,
			name,
			price,
			category_id,
			created_at::text,
			updated_at::text
		FROM products
		WHERE id = $1
	`

	err := r.db.QueryRow(context.Background(), query, req.Id).Scan(
		&product.Id,
		&product.Name,
		&product.Price,
		&product.Category_id,
		&product.CreatedAt,
		&product.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &product, nil
}

func (r *productRepo) GetList(req *models.ProductGetListRequest) (*models.ProductGetListResponse, error) {
	var (
		resp   = &models.ProductGetListResponse{}
		query  string
		where  = " WHERE TRUE"
		offset = " OFFSET 0"
		limit  = " LIMIT 10"
	)

	query = `
		SELECT
			id,
			name,
			price,
			category_id,
			created_at::text,
			updated_at::text
		FROM products
	`

	countQ := `SELECT COUNT(*) FROM products;`

	err := r.db.QueryRow(context.Background(), countQ).Scan(&resp.Count)
	if err != nil {
		return resp, err
	}

	if req.Offset > 0 {
		offset = fmt.Sprintf(" OFFSET %d", req.Offset)
	}

	if req.Limit > 0 {
		limit = fmt.Sprintf(" LIMIT %d", req.Limit)
	}

	if req.Search != "" {
		where += ` AND name ILIKE '%' || '` + req.Search + `' || '%'`
	}

	query += where + offset + limit

	fmt.Println(query)

	rows, err := r.db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var product models.Product

		err := rows.Scan(
			&product.Id,
			&product.Name,
			&product.Price,
			&product.Category_id,
			&product.CreatedAt,
			&product.UpdatedAt,
		)

		if err != nil {
			return nil, err
		}

		resp.Products = append(resp.Products, &product)
	}

	return resp, nil
}

func (r *productRepo) Update(req *models.UpdateProduct) (string, error) {

	var (
		query  string
		params map[string]interface{}
	)

	query = `
		UPDATE
			products
		SET
			name = :name,
			price = :price,
			category_id = :category_id,
			updated_at = NOW()
		WHERE id = :id
	`

	params = map[string]interface{}{
		"id":          req.Id,
		"name":        req.Name,
		"price":       req.Price,
		"category_id": req.Category_id,
	}

	query, args := helper.ReplaceQueryParams(query, params)

	_, err := r.db.Exec(context.Background(), query, args...)
	if err != nil {
		return "", err
	}

	return req.Id, nil
}

func (r *productRepo) Delete(req *models.ProductPrimaryKey) error {

	_, err := r.db.Exec(context.Background(), "DELETE FROM products WHERE id = $1", req.Id)
	if err != nil {
		return err
	}

	return nil
}
