# 🍔 Food Delivery System - Microservices Architecture

> Tiểu luận môn Kiến trúc Phần mềm hướng dịch vụ - UEH
Nhóm Cá mồi ba cô gái
1. Trần Văn Ngân (nhóm trưởng)
2. Nguyễn Trần Thanh Vy
3. Trần Minh Tuấn
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
[Order Service :9001]  ── tạo đơn hàng
    │
    ▼
[Payment Service :9002] ── xử lý thanh toán
    │
    │ publish event "order.paid"
    ▼
[RabbitMQ :5673]
    │
    │ subscribe
    ▼
[Delivery Service :9003] ── tìm tài xế, giao hàng
```

---

## Các Service

| Service | Port | Chức năng |
|---|---|---|
| Order Service | 9001 | Tạo và quản lý đơn hàng |
| Payment Service | 9002 | Xử lý thanh toán, publish event |
| Delivery Service | 9003 | Nhận event, tìm tài xế, giao hàng |
| RabbitMQ Dashboard | 15673 | Quản lý message queue |

---

## Cách chạy

### Yêu cầu
- Docker Desktop đã cài và đang chạy

### Chạy hệ thống
```bash
docker compose up --build -d
```

### Truy cập Swagger UI
- Order: http://localhost:9001/docs
- Payment: http://localhost:9002/docs  
- Delivery: http://localhost:9003/docs
- RabbitMQ: http://localhost:15673 (guest/guest)

---

## Luồng nghiệp vụ demo

1. **Tạo đơn hàng** → POST http://localhost:9001/orders
2. **Thanh toán** → POST http://localhost:9002/payments
3. **Xem tài xế được phân công** → GET http://localhost:9003/deliveries
4. **Tài xế cập nhật trạng thái** → PUT http://localhost:9003/deliveries/{id}/vi-tri

---

## Công nghệ sử dụng

- **Python 3.11** + **FastAPI** — framework backend
- **RabbitMQ** — message broker cho Event-Driven
- **Docker** + **Docker Compose** — containerization
- **Pydantic** — validation dữ liệu
- **aio-pika** — thư viện kết nối RabbitMQ bất đồng bộ
