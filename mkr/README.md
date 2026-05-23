# Apache Kafka Request-Reply Demo

## Опис

У цьому проєкті реалізовано шаблон request-reply через Apache Kafka.

Проєкт складається з двох окремих Java-сервісів:

- Producer — надсилає запит із діапазоном чисел start,finish у топік demo-requests.
- Consumer — отримує запит, обчислює середню кількість кроків за гіпотезою Колатца та надсилає відповідь у топік demo-responses.

Для зв’язку запиту й відповіді використовується correlation-id у headers Kafka-повідомлення.

## Топіки

- demo-requests
- demo-responses

## Запуск

Створити Docker-мережу:

```bash
docker network create kafka-net