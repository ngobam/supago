package domain

import "time"

type Blogs struct {
	Id          int64      `db:"id" json:"id"`
	Title       *string    `db:"title" json:"title"`
	Description *string    `db:"description" json:"description"`
	Content     *string    `db:"content" json:"content"`
	Tags        *string    `db:"tags" json:"tags"`
	Status      *string    `db:"status" json:"status"`
	Category    *string    `db:"category" json:"category"`
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
	AuthorId    *string    `db:"author_id" json:"author_id"`
	Date        *time.Time `db:"date" json:"date"`
	Type        *string    `db:"type" json:"type"`
	Thumbnail   *string    `db:"thumbnail" json:"thumbnail"`
}
