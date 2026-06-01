# ============================================================
# DELIVERY SERVICE - Nhận event + Xử lý giao hàng + Publish event hoàn thành
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
        logging.FileHandler("delivery_service.log")
    ]
)
logger = logging.getLogger("delivery_service")

# ---- Lưu tạm dữ liệu trong memory ----
# NOTE: Production nên thay bằng PostgreSQL/Redis để persist qua restart
giao_hang_db = {}

# Giả lập danh sách tài xế
tai_xe_co_san = [
    {"tai_xe_id": "TX001", "ten": "Nguyễn Văn A", "so_dien_thoai": "0901234567", "vi_tri": "Quận 1"},
    {"tai_xe_id": "TX002", "ten": "Trần Thị B",   "so_dien_thoai": "0912345678", "vi_tri": "Quận 3"},
    {"tai_xe_id": "TX003", "ten": "Lê Văn C",     "so_dien_thoai": "0923456789", "vi_tri": "Quận 7"},
]

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
rabbit_connection = None
rabbit_channel = None        # channel dùng để CONSUME (nhận event)
rabbit_publish_channel = None  # channel riêng để PUBLISH (gửi event) — Fix Lỗi 3


# ============================================================
# EVENT PUBLISHER — Gửi event ngược về Order Service
# ============================================================

async def publish_event(ten_queue: str, du_lieu: dict):
    """
    Gửi event lên RabbitMQ qua channel RIÊNG (Fix Lỗi 3: tách publish channel khỏi consume channel).
    aio_pika khuyến nghị không dùng chung channel cho cả consume lẫn publish.
    """
    global rabbit_publish_channel
    if rabbit_publish_channel is None:
        logger.warning(f"[DELIVERY] Chưa kết nối RabbitMQ, bỏ qua event: {ten_queue}")
        return
    try:
        await rabbit_publish_channel.declare_queue(ten_queue, durable=True)
        tin_nhan = json.dumps(du_lieu, ensure_ascii=False)
        await rabbit_publish_channel.default_exchange.publish(
            aio_pika.Message(body=tin_nhan.encode(), content_type="application/json"),
            routing_key=ten_queue
        )
        logger.info(f"[DELIVERY] Đã publish event '{ten_queue}' cho đơn: {du_lieu.get('don_hang_id')}")
    except Exception as e:
        logger.error(f"[DELIVERY] Lỗi publish event: {e}")


# ============================================================
# EVENT CONSUMER — Nhận event 'order.paid' từ Payment Service
# ============================================================

async def xu_ly_event(tin_nhan: aio_pika.IncomingMessage):
    """
    Khi nhận 'order.paid' → phân công tài xế → publish 'order.assigned' về Order Service.
    Đây là phần khép kín vòng lặp Event-Driven:
    Payment → [order.paid] → Delivery → [order.assigned] → Order
    """
    async with tin_nhan.process():
        try:
            du_lieu = json.loads(tin_nhan.body.decode())
            logger.info(f"[DELIVERY] Nhận event: {du_lieu.get('event')}")

            if du_lieu["event"] == "order.paid":
                don_hang_id = du_lieu["don_hang_id"]

                # Tự động tìm tài xế gần nhất (giả lập)
                import random
                tai_xe = random.choice(tai_xe_co_san)

                giao_hang_id = str(uuid.uuid4())[:8].upper()
                giao_hang = {
                    "giao_hang_id": giao_hang_id,
                    "don_hang_id": don_hang_id,
                    "tai_xe": tai_xe,
                    "trang_thai": "dang_toi_nha_hang",
                    "thoi_gian_bat_dau": datetime.datetime.now().isoformat(),
                    "thoi_gian_du_kien": "30-45 phút",
                    "vi_tri_hien_tai": tai_xe["vi_tri"]
                }
                giao_hang_db[giao_hang_id] = giao_hang
                logger.info(f"[DELIVERY] Phân công tài xế {tai_xe['ten']} cho đơn {don_hang_id}")

                # Publish event 'order.assigned' → Order Service cập nhật trạng thái sang dang_giao
                await publish_event("order.status.update", {
                    "event": "order.assigned",
                    "don_hang_id": don_hang_id,
                    "giao_hang_id": giao_hang_id,
                    "tai_xe": tai_xe["ten"],
                    "thoi_gian": datetime.datetime.now().isoformat()
                })

        except Exception as e:
            logger.error(f"[DELIVERY] Lỗi xử lý event: {e}")


async def ket_noi_va_lang_nghe():
    """
    Kết nối RabbitMQ và khởi tạo 2 channel riêng biệt:
    - consume_channel: chỉ dùng để NHẬN event (subscribe)
    - publish_channel: chỉ dùng để GỬI event (Fix Lỗi 3 — aio_pika best practice)
    """
    global rabbit_connection, rabbit_channel, rabbit_publish_channel
    try:
        rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)

        # Channel 1: dùng để consume (nhận event order.paid)
        rabbit_channel = await rabbit_connection.channel()
        queue = await rabbit_channel.declare_queue("order.paid", durable=True)
        await queue.consume(xu_ly_event)

        # Channel 2: dùng riêng để publish (gửi event về Order Service)
        rabbit_publish_channel = await rabbit_connection.channel()

        logger.info(f"[DELIVERY] Đang lắng nghe 'order.paid' từ RabbitMQ: {RABBITMQ_URL}")
    except Exception as e:
        logger.error(f"[DELIVERY] Lỗi kết nối RabbitMQ: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(ket_noi_va_lang_nghe())
    logger.info("Delivery Service khởi động tại port 9003")
    yield
    if rabbit_connection and not rabbit_connection.is_closed:
        await rabbit_connection.close()
        logger.info("[DELIVERY] Đã đóng kết nối RabbitMQ")


app = FastAPI(
    title="Delivery Service",
    description="Service tìm tài xế, xử lý giao hàng, publish event hoàn thành về Order Service",
    version="1.0.0",
    lifespan=lifespan
)


# ---- Schema dữ liệu ----
class CapNhatViTri(BaseModel):
    vi_tri_hien_tai: str
    trang_thai: str  # dang_toi_nha_hang | da_lay_hang | dang_giao | da_giao


# ---- API Endpoints ----

@app.get("/")
def trang_chu():
    return {"message": "Delivery Service đang chạy!", "version": "1.0.0"}


@app.get("/deliveries")
def xem_tat_ca_giao_hang():
    """Xem tất cả đơn giao hàng"""
    logger.info(f"Lấy danh sách giao hàng, tổng số: {len(giao_hang_db)}")
    return {"tong_so": len(giao_hang_db), "giao_hang": list(giao_hang_db.values())}


@app.get("/deliveries/{giao_hang_id}")
def xem_giao_hang(giao_hang_id: str):
    """Xem thông tin đơn giao hàng"""
    if giao_hang_id not in giao_hang_db:
        logger.warning(f"Không tìm thấy đơn giao hàng: {giao_hang_id}")
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn giao hàng")
    return giao_hang_db[giao_hang_id]


@app.put("/deliveries/{giao_hang_id}/vi-tri")
async def cap_nhat_vi_tri(giao_hang_id: str, request: CapNhatViTri):
    """
    Tài xế cập nhật vị trí và trạng thái giao hàng.
    Khi trạng thái là 'da_giao' → publish event 'delivery.completed' về Order Service
    để Order Service tự động cập nhật đơn hàng sang 'da_giao'.
    """
    if giao_hang_id not in giao_hang_db:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn giao hàng")

    cac_trang_thai = ["dang_toi_nha_hang", "da_lay_hang", "dang_giao", "da_giao"]
    if request.trang_thai not in cac_trang_thai:
        raise HTTPException(status_code=400, detail="Trạng thái không hợp lệ")

    giao_hang_db[giao_hang_id]["trang_thai"] = request.trang_thai
    giao_hang_db[giao_hang_id]["vi_tri_hien_tai"] = request.vi_tri_hien_tai

    if request.trang_thai == "da_giao":
        giao_hang_db[giao_hang_id]["thoi_gian_hoan_thanh"] = datetime.datetime.now().isoformat()

        # Publish event 'delivery.completed' → Order Service cập nhật đơn hàng sang 'da_giao'
        don_hang_id = giao_hang_db[giao_hang_id]["don_hang_id"]
        await publish_event("order.status.update", {
            "event": "delivery.completed",
            "don_hang_id": don_hang_id,
            "giao_hang_id": giao_hang_id,
            "thoi_gian": datetime.datetime.now().isoformat()
        })
        logger.info(f"[DELIVERY] Giao hàng hoàn tất, đã publish delivery.completed cho đơn {don_hang_id}")

    logger.info(f"Đơn {giao_hang_id}: {request.trang_thai} tại {request.vi_tri_hien_tai}")
    return {
        "message": "Cập nhật thành công",
        "giao_hang_id": giao_hang_id,
        "trang_thai": request.trang_thai
    }


@app.get("/drivers")
def xem_tai_xe():
    """Xem danh sách tài xế có sẵn"""
    return {"tai_xe": tai_xe_co_san}
