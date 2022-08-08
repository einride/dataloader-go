Dataloader Go
=============

A generic implementation of a Facebook's [dataloader](https://github.com/facebook/dataloader) pattern, based on [vektah](https://github.com/vektah) pre-generics implementation [dataloaden](https://github.com/vektah/dataloaden).

Example
-------

```go
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
		Wait:     0,
		MaxBatch: 0,
	})
	users, err := loader.LoadAll([]string{"foo", "bar"})
	if err != nil {
		panic(err)
	}
	fmt.Println(users)
	// Output: [{foo} {bar}]
}
```
