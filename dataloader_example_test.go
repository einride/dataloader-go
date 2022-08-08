package dataloader_test

import (
	"context"
	"fmt"
	"time"

	"go.einride.tech/dataloader"
)

func ExampleDataloader() {
	type User struct {
		ID string
	}
	ctx := context.Background()
	loader := dataloader.New[User](ctx, dataloader.Config[User]{
		Fetch: func(ctx context.Context, keys []string) ([]User, error) {
			users := make([]User, len(keys))
			for i, key := range keys {
				users[i] = User{ID: key}
			}
			return users, nil
		},
		Wait:     2 * time.Millisecond,
		MaxBatch: 100,
	})
	users, err := loader.LoadAll([]string{"foo", "bar"})
	if err != nil {
		panic(err)
	}
	fmt.Println(users)
	// Output: [{foo} {bar}]
}
