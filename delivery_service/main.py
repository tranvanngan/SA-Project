# ============================================================
# DELIVERY SERVICE - Nhận event + Xử lý giao hàng
# Môn: Kiến trúc Phần mềm hướng dịch vụ - UEH
# ============================================================

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import uuid
import datetime
import json
import aio_pika
import asyncio

app = FastAPI(
    title="Delivery Service",
    description="Service tìm tài xế và xử lý giao hàng, lắng nghe event từ Payment Service",
    version="1.0.0"
)

# ---- Lưu tạm dữ liệu trong memory ----
giao_hang_db = {}

# Giả lập danh sách tài xế
tai_xe_co_san = [
    {"tai_xe_id": "TX001", "ten": "Nguyễn Văn A", "so_dien_thoai": "0901234567", "vi_tri": "Quận 1"},
    {"tai_xe_id": "TX002", "ten": "Trần Thị B", "so_dien_thoai": "0912345678", "vi_tri": "Quận 3"},
    {"tai_xe_id": "TX003", "ten": "Lê Văn C", "so_dien_thoai": "0923456789", "vi_tri": "Quận 7"},
]

# ---- Kết nối RabbitMQ ----
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
rabbit_connection = None

async def ket_noi_va_lang_nghe():
    """
    Kết nối RabbitMQ và bắt đầu lắng nghe event 'order.paid'.
    Đây là Subscribe trong mô hình Publish/Subscribe.
    """
    global rabbit_connection
    try:
        rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel = await rabbit_connection.channel()
        
        # Lắng nghe queue 'order.paid'
        queue = await channel.declare_queue("order.paid", durable=True)
        
        print("[DELIVERY] Đang lắng nghe event 'order.paid' từ RabbitMQ...")
        
        # Khi có tin nhắn mới thì gọi hàm xu_ly_event
        await queue.consume(xu_ly_event)
        
    except Exception as e:
        print(f"[DELIVERY] Lỗi kết nối RabbitMQ: {e}")


async def xu_ly_event(tin_nhan: aio_pika.IncomingMessage):
    """
    Xử lý event nhận được từ Payment Service.
    Khi thanh toán thành công, tự động tìm tài xế giao hàng.
    """
    async with tin_nhan.process():
        try:
            # Đọc dữ liệu từ message
            du_lieu = json.loads(tin_nhan.body.decode())
            print(f"[DELIVERY] Nhận được event: {du_lieu['event']}")
            
            if du_lieu["event"] == "order.paid":
                don_hang_id = du_lieu["don_hang_id"]
                
                # Tự động tìm tài xế gần nhất (giả lập)
                import random
                tai_xe = random.choice(tai_xe_co_san)
                
                # Tạo đơn giao hàng
                giao_hang_id = str(uuid.uuid4())[:8].upper()
                giao_hang = {
                    "giao_hang_id": giao_hang_id,
                    "don_hang_id": don_hang_id,
                    "tai_xe": tai_xe,
                    "trang_thai": "dang_toi_nha_hang",
                    "thoi_gian_bat_dau": datetime.datetime.now().isoformat(),
                    "thoi_gian_du_kien": "30-45 phút"
                }
                
                giao_hang_db[giao_hang_id] = giao_hang
                print(f"[DELIVERY] Đã phân công tài xế {tai_xe['ten']} cho đơn {don_hang_id}")
                
        except Exception as e:
            print(f"[DELIVERY] Lỗi xử lý event: {e}")


@app.on_event("startup")
async def startup():
    # Chạy lắng nghe RabbitMQ trong background
    asyncio.create_task(ket_noi_va_lang_nghe())

@app.on_event("shutdown")
async def shutdown():
    if rabbit_connection:
        await rabbit_connection.close()


# ---- Schema dữ liệu ----
class CapNhatViTri(BaseModel):
    vi_tri_hien_tai: str
    trang_thai: str


# ---- API Endpoints ----

@app.get("/")
def trang_chu():
    return {"message": "Delivery Service đang chạy!", "version": "1.0.0"}


@app.get("/deliveries")
def xem_tat_ca_giao_hang():
    """Xem tất cả đơn giao hàng"""
    return {
        "tong_so": len(giao_hang_db),
        "giao_hang": list(giao_hang_db.values())
    }


@app.get("/deliveries/{giao_hang_id}")
def xem_giao_hang(giao_hang_id: str):
    """Xem thông tin đơn giao hàng"""
    if giao_hang_id not in giao_hang_db:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn giao hàng")
    return giao_hang_db[giao_hang_id]


@app.put("/deliveries/{giao_hang_id}/vi-tri")
def cap_nhat_vi_tri(giao_hang_id: str, request: CapNhatViTri):
    """
    Tài xế cập nhật vị trí và trạng thái giao hàng.
    Trong thực tế sẽ push notification cho khách hàng.
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
    
    print(f"[DELIVERY] Đơn {giao_hang_id}: {request.trang_thai} tại {request.vi_tri_hien_tai}")
    
    return {
        "message": "Cập nhật thành công",
        "giao_hang_id": giao_hang_id,
        "trang_thai": request.trang_thai
    }


@app.get("/drivers")
def xem_tai_xe():
    """Xem danh sách tài xế có sẵn"""
    return {"tai_xe": tai_xe_co_san}
