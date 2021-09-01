cd go
go test -coverprofile=../cover.out ./v2/... -v
go tool cover -html=../cover.out -o ../cover.html
cd ..