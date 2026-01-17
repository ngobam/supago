package domain

import "time"

type Profiles struct {
	Id        string     `db:"id" json:"id"`
	Username  *string    `db:"username" json:"username"`
	FullName  *string    `db:"full_name" json:"full_name"`
	AvatarUrl *string    `db:"avatar_url" json:"avatar_url"`
	Bio       *string    `db:"bio" json:"bio"`
	CreatedAt *time.Time `db:"created_at" json:"created_at"`
	UpdatedAt *time.Time `db:"updated_at" json:"updated_at"`
}
