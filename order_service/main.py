# ============================================================
# ORDER SERVICE - Xử lý đơn đặt hàng
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
import aio_pika
import asyncio
import logging

# ---- Cấu hình logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("order_service.log")
    ]
)
logger = logging.getLogger("order_service")

# ---- Lưu tạm đơn hàng trong memory ----
# NOTE: Production nên thay bằng PostgreSQL/Redis để persist qua restart
don_hang_db = {}

# ---- Trạng thái hợp lệ theo State Machine ----
CHUYEN_TRANG_THAI_HOP_LE = {
    "cho_xac_nhan":  ["dang_chuan_bi", "dang_giao", "da_huy"],  # dang_giao: khi Payment xác nhận và Delivery phân công tài xế
    "dang_chuan_bi": ["dang_giao"],
    "dang_giao":     ["da_giao"],
    "da_giao":       [],
    "da_huy":        []
}

# ---- RabbitMQ ----
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
rabbit_connection = None


# ---- Schema dữ liệu ----
class TaoDonHang(BaseModel):
    khach_hang_id: str
    nha_hang_id: str
    danh_sach_mon: list[str]
    dia_chi_giao: str
    ghi_chu: Optional[str] = None

class DonHang(BaseModel):
    don_hang_id: str
    khach_hang_id: str
    nha_hang_id: str
    danh_sach_mon: list[str]
    dia_chi_giao: str
    trang_thai: str
    tong_tien: float
    thoi_gian_tao: str
    ghi_chu: Optional[str] = None


# ---- Tính tiền đơn giản ----
def tinh_tien(danh_sach_mon: list[str]) -> float:
    return len(danh_sach_mon) * 50000


# ============================================================
# EVENT CONSUMER — Nhận event từ Delivery Service
# ============================================================

async def xu_ly_event_delivery(tin_nhan: aio_pika.IncomingMessage):
    """
    Lắng nghe event 'delivery.completed' từ Delivery Service.
    Khi tài xế giao hàng thành công → tự động cập nhật trạng thái đơn hàng sang 'da_giao'.
    Đây là phần khép kín vòng lặp Event-Driven giữa Order ↔ Delivery.
    """
    async with tin_nhan.process():
        try:
            du_lieu = json.loads(tin_nhan.body.decode())
            logger.info(f"[ORDER] Nhận event: {du_lieu.get('event')}")

            if du_lieu["event"] == "delivery.completed":
                don_hang_id = du_lieu["don_hang_id"]
                if don_hang_id in don_hang_db:
                    don_hang_db[don_hang_id]["trang_thai"] = "da_giao"
                    logger.info(f"[ORDER] Đơn hàng {don_hang_id} → da_giao (do delivery.completed)")
                else:
                    logger.warning(f"[ORDER] Nhận event nhưng không tìm thấy đơn hàng: {don_hang_id}")

            elif du_lieu["event"] == "order.assigned":
                # Delivery Service vừa phân công tài xế → chuyển sang dang_giao
                don_hang_id = du_lieu["don_hang_id"]
                if don_hang_id in don_hang_db:
                    trang_thai_hien_tai = don_hang_db[don_hang_id]["trang_thai"]
                    if "dang_giao" in CHUYEN_TRANG_THAI_HOP_LE.get(trang_thai_hien_tai, []):
                        don_hang_db[don_hang_id]["trang_thai"] = "dang_giao"
                        logger.info(f"[ORDER] Đơn hàng {don_hang_id} → dang_giao (do order.assigned)")

            elif du_lieu["event"] == "order.payment_failed":
                # Fix Lỗi 2: Payment thất bại → tự động hủy đơn hàng
                don_hang_id = du_lieu["don_hang_id"]
                if don_hang_id in don_hang_db:
                    trang_thai_hien_tai = don_hang_db[don_hang_id]["trang_thai"]
                    if "da_huy" in CHUYEN_TRANG_THAI_HOP_LE.get(trang_thai_hien_tai, []):
                        don_hang_db[don_hang_id]["trang_thai"] = "da_huy"
                        logger.info(f"[ORDER] Đơn hàng {don_hang_id} → da_huy (thanh toán thất bại)")

        except Exception as e:
            logger.error(f"[ORDER] Lỗi xử lý event delivery: {e}")


async def ket_noi_va_lang_nghe():
    """Kết nối RabbitMQ và subscribe các event từ Delivery Service"""
    global rabbit_connection
    try:
        rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel = await rabbit_connection.channel()

        # Lắng nghe queue 'order.status.update' — Delivery Service gửi về đây
        queue = await channel.declare_queue("order.status.update", durable=True)
        await queue.consume(xu_ly_event_delivery)

        logger.info("[ORDER] Đang lắng nghe event từ Delivery Service trên queue 'order.status.update'")
    except Exception as e:
        logger.error(f"[ORDER] Lỗi kết nối RabbitMQ: {e}")


# ---- Lifespan ----
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(ket_noi_va_lang_nghe())
    logger.info("Order Service khởi động tại port 9001")
    yield
    if rabbit_connection and not rabbit_connection.is_closed:
        await rabbit_connection.close()
        logger.info("[ORDER] Đã đóng kết nối RabbitMQ")


app = FastAPI(
    title="Order Service",
    description="Service xử lý đơn đặt hàng cho hệ thống Food Delivery",
    version="1.0.0",
    lifespan=lifespan
)


# ---- API Endpoints ----

@app.get("/")
def trang_chu():
    return {"message": "Order Service đang chạy!", "version": "1.0.0"}


@app.post("/orders", response_model=DonHang)
def tao_don_hang(request: TaoDonHang):
    """Tạo đơn hàng mới."""
    don_hang_id = str(uuid.uuid4())[:8].upper()
    don_hang = {
        "don_hang_id": don_hang_id,
        "khach_hang_id": request.khach_hang_id,
        "nha_hang_id": request.nha_hang_id,
        "danh_sach_mon": request.danh_sach_mon,
        "dia_chi_giao": request.dia_chi_giao,
        "trang_thai": "cho_xac_nhan",
        "tong_tien": tinh_tien(request.danh_sach_mon),
        "thoi_gian_tao": datetime.datetime.now().isoformat(),
        "ghi_chu": request.ghi_chu
    }
    don_hang_db[don_hang_id] = don_hang
    logger.info(f"Đơn hàng mới: {don_hang_id} - Khách: {request.khach_hang_id} - Tổng: {don_hang['tong_tien']:,.0f} VND")
    return don_hang


@app.get("/orders/{don_hang_id}", response_model=DonHang)
def xem_don_hang(don_hang_id: str):
    """Xem thông tin đơn hàng theo ID"""
    if don_hang_id not in don_hang_db:
        logger.warning(f"Không tìm thấy đơn hàng: {don_hang_id}")
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn hàng")
    return don_hang_db[don_hang_id]


@app.get("/orders")
def xem_tat_ca_don_hang():
    """Xem tất cả đơn hàng (dành cho admin)"""
    logger.info(f"Lấy danh sách đơn hàng, tổng số: {len(don_hang_db)}")
    return {"tong_so": len(don_hang_db), "don_hang": list(don_hang_db.values())}


@app.put("/orders/{don_hang_id}/trang-thai")
def cap_nhat_trang_thai(don_hang_id: str, trang_thai_moi: str):
    """
    Cập nhật trạng thái đơn hàng theo State Machine.
    Được gọi bởi các service khác (Payment, Delivery).
    """
    if don_hang_id not in don_hang_db:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn hàng")

    trang_thai_hien_tai = don_hang_db[don_hang_id]["trang_thai"]
    trang_thai_co_the_chuyen = CHUYEN_TRANG_THAI_HOP_LE.get(trang_thai_hien_tai, [])
    if trang_thai_moi not in trang_thai_co_the_chuyen:
        raise HTTPException(
            status_code=400,
            detail=f"Không thể chuyển từ '{trang_thai_hien_tai}' sang '{trang_thai_moi}'"
        )

    don_hang_db[don_hang_id]["trang_thai"] = trang_thai_moi
    logger.info(f"Đơn hàng {don_hang_id}: {trang_thai_hien_tai} -> {trang_thai_moi}")
    return {"message": "Cập nhật thành công", "trang_thai": trang_thai_moi}


@app.delete("/orders/{don_hang_id}")
def huy_don_hang(don_hang_id: str):
    """Hủy đơn hàng (chỉ được khi còn đang chờ xác nhận)"""
    if don_hang_id not in don_hang_db:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn hàng")

    don = don_hang_db[don_hang_id]
    if don["trang_thai"] != "cho_xac_nhan":
        raise HTTPException(status_code=400, detail="Chỉ có thể hủy đơn hàng khi đang chờ xác nhận")

    don_hang_db[don_hang_id]["trang_thai"] = "da_huy"
    logger.info(f"Hủy đơn hàng: {don_hang_id}")
    return {"message": "Đã hủy đơn hàng thành công"}
