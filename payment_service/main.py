# ============================================================
# PAYMENT SERVICE - Xử lý thanh toán + Publish Event
# Môn: Kiến trúc Phần mềm hướng dịch vụ - UEH
# ============================================================

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Optional
import uuid
import datetime
import json
import os
import aio_pika  # thư viện kết nối RabbitMQ
import asyncio
import logging

# ---- Cấu hình logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("payment_service.log")
    ]
)
logger = logging.getLogger("payment_service")

# ---- Lưu tạm giao dịch trong memory ----
giao_dich_db = {}

# ---- Đọc URL RabbitMQ từ biến môi trường (không hardcode) ----
# docker-compose.yml đã truyền: RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

rabbit_connection = None
rabbit_channel = None


async def ket_noi_rabbitmq():
    """Kết nối tới RabbitMQ khi service khởi động"""
    global rabbit_connection, rabbit_channel
    try:
        rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
        rabbit_channel = await rabbit_connection.channel()
        logger.info(f"Kết nối RabbitMQ thành công: {RABBITMQ_URL}")
    except Exception as e:
        logger.error(f"Lỗi kết nối RabbitMQ: {e}")
        # Nếu không kết nối được thì vẫn chạy bình thường
        # chỉ không publish event được thôi


async def publish_event(ten_queue: str, du_lieu: dict):
    """
    Gửi event lên RabbitMQ để các service khác nhận.
    Đây là trái tim của Event-Driven Architecture.
    """
    global rabbit_channel
    if rabbit_channel is None:
        logger.warning(f"Chưa kết nối RabbitMQ, bỏ qua event: {ten_queue}")
        return

    try:
        # Khai báo queue (tạo nếu chưa có)
        await rabbit_channel.declare_queue(ten_queue, durable=True)

        # Chuyển dữ liệu sang JSON rồi gửi
        tin_nhan = json.dumps(du_lieu, ensure_ascii=False)
        await rabbit_channel.default_exchange.publish(
            aio_pika.Message(
                body=tin_nhan.encode(),
                content_type="application/json"
            ),
            routing_key=ten_queue
        )
        logger.info(f"Đã publish event '{ten_queue}' cho đơn: {du_lieu.get('don_hang_id')}")
    except Exception as e:
        logger.error(f"Lỗi publish event: {e}")


# ---- Lifespan thay cho @app.on_event đã bị deprecated ----
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Chạy khi khởi động
    await ket_noi_rabbitmq()
    logger.info("Payment Service khởi động tại port 9002")
    yield
    # Chạy khi tắt
    if rabbit_connection and not rabbit_connection.is_closed:
        await rabbit_connection.close()
        logger.info("Đã đóng kết nối RabbitMQ")


app = FastAPI(
    title="Payment Service",
    description="Service xử lý thanh toán và thông báo cho các service khác qua RabbitMQ",
    version="1.0.0",
    lifespan=lifespan
)


# ---- Schema dữ liệu ----
class YeuCauThanhToan(BaseModel):
    don_hang_id: str
    khach_hang_id: str
    so_tien: float
    phuong_thuc: str  # momo, zalopay, tien_mat, the_ngan_hang

class KetQuaThanhToan(BaseModel):
    giao_dich_id: str
    don_hang_id: str
    so_tien: float
    phuong_thuc: str
    trang_thai: str  # thanh_cong, that_bai, cho_xu_ly
    thoi_gian: str
    thong_bao: str


# ---- Giả lập xử lý thanh toán ----
def xu_ly_thanh_toan_gia_lap(phuong_thuc: str, so_tien: float) -> bool:
    """
    Giả lập gọi API cổng thanh toán bên thứ 3.
    Trong thực tế sẽ gọi Momo API, ZaloPay API...
    """
    # Giả lập: thanh toán tiền mặt luôn thành công
    # Các phương thức khác thành công 90% thời gian
    import random
    if phuong_thuc == "tien_mat":
        return True
    return random.random() > 0.1  # 90% thành công


# ---- API Endpoints ----

@app.get("/")
def trang_chu():
    return {"message": "Payment Service đang chạy!", "version": "1.0.0"}


@app.post("/payments", response_model=KetQuaThanhToan)
async def xu_ly_thanh_toan(request: YeuCauThanhToan):
    """
    Xử lý thanh toán cho đơn hàng.

    Luồng xử lý:
    1. Nhận yêu cầu thanh toán
    2. Gọi cổng thanh toán (giả lập)
    3. Nếu thành công -> publish event 'order.paid' lên RabbitMQ
    4. Delivery Service sẽ nhận event và bắt đầu tìm tài xế
    """
    giao_dich_id = str(uuid.uuid4())[:8].upper()
    thoi_gian = datetime.datetime.now().isoformat()

    # Bước 2: Xử lý thanh toán
    thanh_cong = xu_ly_thanh_toan_gia_lap(request.phuong_thuc, request.so_tien)

    if thanh_cong:
        trang_thai = "thanh_cong"
        thong_bao = f"Thanh toán {request.so_tien:,.0f} VND qua {request.phuong_thuc} thành công!"

        # Bước 3: Publish event để Delivery Service biết mà xử lý
        event_data = {
            "event": "order.paid",  # tên event
            "giao_dich_id": giao_dich_id,
            "don_hang_id": request.don_hang_id,
            "khach_hang_id": request.khach_hang_id,
            "so_tien": request.so_tien,
            "thoi_gian": thoi_gian
        }
        await publish_event("order.paid", event_data)

    else:
        trang_thai = "that_bai"
        thong_bao = "Thanh toán thất bại. Vui lòng thử lại!"

        # Fix Lỗi 2: Notify Order Service khi thanh toán thất bại → hủy đơn
        await publish_event("order.status.update", {
            "event": "order.payment_failed",
            "don_hang_id": request.don_hang_id,
            "ly_do": "Thanh toán thất bại",
            "thoi_gian": thoi_gian
        })

    # Lưu giao dịch
    ket_qua = {
        "giao_dich_id": giao_dich_id,
        "don_hang_id": request.don_hang_id,
        "so_tien": request.so_tien,
        "phuong_thuc": request.phuong_thuc,
        "trang_thai": trang_thai,
        "thoi_gian": thoi_gian,
        "thong_bao": thong_bao
    }
    giao_dich_db[giao_dich_id] = ket_qua

    logger.info(f"Giao dịch {giao_dich_id}: {trang_thai} - Đơn hàng: {request.don_hang_id}")
    return ket_qua


@app.get("/payments/{giao_dich_id}")
def xem_giao_dich(giao_dich_id: str):
    """Xem thông tin giao dịch theo ID"""
    if giao_dich_id not in giao_dich_db:
        logger.warning(f"Không tìm thấy giao dịch: {giao_dich_id}")
        raise HTTPException(status_code=404, detail="Không tìm thấy giao dịch")
    return giao_dich_db[giao_dich_id]


@app.get("/payments")
def xem_tat_ca_giao_dich():
    """Xem tất cả giao dịch"""
    logger.info(f"Lấy danh sách giao dịch, tổng số: {len(giao_dich_db)}")
    return {
        "tong_so": len(giao_dich_db),
        "giao_dich": list(giao_dich_db.values())
    }
