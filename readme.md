# oh no

## basic types

```rust
type User {
  id: Int,
  firstname: String,
  lastname: String
}
```

```sql
select firstname, lastname from user where id = 1;
```

## sum types

```rust
type Color { 
  RGB {
    red: Int,
    green: Int,
    blue: Int
  }, 
  Greyscale {
    greyscale:Int
  }
}
```

Select via constructor to ignore other constructors

```sql
select RGB{red, green} from Color; # red: int, green: int
select Greyscale{greyscale} from color; # greyscale: int
```

Some sort of eventual pattern matching syntax feels inevitable.

## dealing with overlapping columns

For ergonomics, we probably want to be able to select across constructors in a
lens-like fashion too.

```rust
type MultiUser {
  InternalUser { id: int, name: string },
  ExternalUser { id: int, email: string }
}
```

```sql
select id from MultiUser; # id: int
select email from MultiUser; # email: Maybe<string> --? not sure about this so
much
```

