# 🍔 Food Delivery System - Microservices Architecture

> Tiểu luận môn Thiết kế Kiến trúc Phần mềm - UEH  
> Nhóm 3

---

## Giới thiệu hệ thống

Hệ thống đặt đồ ăn trực tuyến được xây dựng theo kiến trúc **Microservices + Event-Driven**.  
Gồm 3 service độc lập giao tiếp với nhau qua **RabbitMQ**.

---

## Kiến trúc tổng quan

```
Khách hàng
    │
    ▼
[Order Service :8001]  ──── tạo đơn hàng
    │
    ▼
[Payment Service :8002] ──── xử lý thanh toán
    │
    │ publish event "order.paid"
    ▼
[RabbitMQ :5672]
    │
    │ subscribe
    ▼
[Delivery Service :8003] ──── tìm tài xế, giao hàng
```

---

## Các Service

| Service | Port | Chức năng |
|---|---|---|
| Order Service | 8001 | Tạo và quản lý đơn hàng |
| Payment Service | 8002 | Xử lý thanh toán, publish event |
| Delivery Service | 8003 | Nhận event, tìm tài xế, giao hàng |
| RabbitMQ Dashboard | 15672 | Quản lý message queue |

---

## Cách chạy

### Yêu cầu
- Docker Desktop đã cài và đang chạy

### Chạy hệ thống
```bash
docker compose up --build -d
```

### Truy cập Swagger UI
- Order: http://localhost:8001/docs
- Payment: http://localhost:8002/docs  
- Delivery: http://localhost:8003/docs
- RabbitMQ: http://localhost:15672 (guest/guest)

---

## Luồng nghiệp vụ demo

1. **Tạo đơn hàng** → POST http://localhost:8001/orders
2. **Thanh toán** → POST http://localhost:8002/payments
3. **Xem tài xế được phân công** → GET http://localhost:8003/deliveries
4. **Tài xế cập nhật trạng thái** → PUT http://localhost:8003/deliveries/{id}/vi-tri

---

## Công nghệ sử dụng

- **Python 3.11** + **FastAPI** — framework backend
- **RabbitMQ** — message broker cho Event-Driven
- **Docker** + **Docker Compose** — containerization
- **Pydantic** — validation dữ liệu
- **aio-pika** — thư viện kết nối RabbitMQ bất đồng bộ
