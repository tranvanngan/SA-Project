# 🍔 Food Delivery System - Microservices Architecture

> Tiểu luận môn Kiến trúc Phần mềm hướng dịch vụ - UEH
Nhóm HeHe
Danh sách thành viên:
1. Trần Văn Ngân (nhóm trưởng)
2. Nguyễn Trần Thanh Vy
3. Trần Minh Tuấn
---
Mục tiêu
Quản lý đơn hàng trực tuyến.
Xử lý thanh toán đơn hàng.
Quản lý quá trình giao hàng.
Minh họa mô hình giao tiếp bất đồng bộ giữa các Microservices.
Đảm bảo khả năng mở rộng và bảo trì hệ thống.
## Kiến trúc tổng quan

```
           Client
             │
             ▼
   [API Gateway :8080]  ← Nginx, entry point duy nhất
    /api/orders   ──────────────────────────────► [Order Service :9001]
    /api/payments ──────────────────────────────► [Payment Service :9002]
    /api/deliveries ────────────────────────────► [Delivery Service :9003]
             
                        Event Flow (RabbitMQ)
                        ─────────────────────────────────────────────
  [Order Service]  ◄── order.status.update ◄── [Delivery Service]
                                                       ▲
  [Payment Service] ──── order.paid ─────────────────►│
```

### Vòng lặp Event-Driven khép kín:
1. Payment Service xử lý thanh toán → publish **`order.paid`**
2. Delivery Service nhận → phân công tài xế → publish **`order.assigned`**
3. Order Service nhận → cập nhật trạng thái → `dang_giao`
4. Tài xế cập nhật `da_giao` → Delivery publish **`delivery.completed`**
5. Order Service nhận → cập nhật trạng thái → `da_giao` ✅

---

## Các Service

| Service      | Port | Chức năng |
|---|---|---|
| API Gateway  | 8080 | Entry point duy nhất, route request đến service |
| Order Service | 9001 | Tạo và quản lý đơn hàng, subscribe event từ Delivery |
| Payment Service | 9002 | Xử lý thanh toán, publish event `order.paid` |
| Delivery Service | 9003 | Nhận event, phân công tài xế, publish event hoàn thành |
| RabbitMQ Dashboard | 15673 | Quản lý message queue |

---

## Cách chạy

### Yêu cầu
- Docker Desktop đã cài và đang chạy

### Chạy hệ thống
```bash
docker compose up --build -d
```

### Truy cập qua API Gateway (khuyến nghị)
```
http://localhost:8080/api/orders
http://localhost:8080/api/payments
http://localhost:8080/api/deliveries
http://localhost:8080/health
```

### Truy cập Swagger UI (dev)
- Order:    http://localhost:9001/docs
- Payment:  http://localhost:9002/docs
- Delivery: http://localhost:9003/docs
- RabbitMQ: http://localhost:15673 (guest/guest)

---

## Luồng nghiệp vụ demo

```bash
# 1. Tạo đơn hàng
POST http://localhost:8080/api/orders

# 2. Thanh toán (Payment tự publish event → Delivery tự phân công tài xế)
POST http://localhost:8080/api/payments

# 3. Xem đơn giao hàng (Delivery đã tự tạo sau khi nhận event)
GET http://localhost:8080/api/deliveries

# 4. Tài xế cập nhật trạng thái da_giao
#    → Delivery tự publish event → Order tự cập nhật sang da_giao
PUT http://localhost:8080/api/deliveries/{id}/vi-tri
```
Đặc trưng kiến trúc đạt được
Loose Coupling

Các dịch vụ không gọi trực tiếp lẫn nhau mà giao tiếp thông qua RabbitMQ.

Scalability

Mỗi service có thể mở rộng độc lập theo nhu cầu.

Fault Isolation

Lỗi ở một service không làm sập toàn bộ hệ thống.

Maintainability

Mỗi service được phát triển và triển khai riêng biệt.
---

## Công nghệ sử dụng

-| Công nghệ      | Mục đích               |
| -------------- | ---------------------- |
| Python 3.11    | Ngôn ngữ lập trình     |
| FastAPI        | Xây dựng REST API      |
| RabbitMQ       | Message Broker         |
| aio-pika       | Kết nối RabbitMQ       |
| Nginx          | API Gateway            |
| Docker         | Containerization       |
| Docker Compose | Orchestration          |
| Pydantic       | Data Validation        |
| GitHub Actions | Continuous Integration |

