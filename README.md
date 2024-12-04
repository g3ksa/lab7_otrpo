### Настройка переменных окружения
.env.example -> .env

### Запуск продюсера и консьюмера
```bash
go mod download
go run producer/producer.go url-to-parse
go run consumer/consumer.go
```