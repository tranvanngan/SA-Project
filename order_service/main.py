# ============================================================
# ORDER SERVICE - Xử lý đơn đặt hàng
# Môn: Kiến trúc Phần mềm hướng dịch vụ - UEH
# ============================================================

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import uuid
import datetime

app = FastAPI(
    title="Order Service",
    description="Service xử lý đơn đặt hàng cho hệ thống Food Delivery",
    version="1.0.0"
)

# ---- Lưu tạm đơn hàng trong memory (thay cho DB) ----
don_hang_db = {}

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
    trang_thai: str  # cho_xac_nhan, dang_chuan_bi, dang_giao, da_giao, da_huy
    tong_tien: float
    thoi_gian_tao: str
    ghi_chu: Optional[str] = None


# ---- Tính tiền đơn giản (giả lập) ----
def tinh_tien(danh_sach_mon: list[str]) -> float:
    # Mỗi món tính 50,000 VND cho đơn giản
    return len(danh_sach_mon) * 50000


# ---- API Endpoints ----

@app.get("/")
def trang_chu():
    return {"message": "Order Service đang chạy!", "version": "1.0.0"}


@app.post("/orders", response_model=DonHang)
def tao_don_hang(request: TaoDonHang):
    """
    Tạo đơn hàng mới.
    Khi khách hàng bấm đặt hàng, request sẽ vào đây.
    """
    # Tạo ID đơn hàng duy nhất
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
    
    # Lưu vào DB tạm
    don_hang_db[don_hang_id] = don_hang
    
    print(f"[ORDER] Đơn hàng mới: {don_hang_id} - Khách: {request.khach_hang_id}")
    
    return don_hang


@app.get("/orders/{don_hang_id}", response_model=DonHang)
def xem_don_hang(don_hang_id: str):
    """Xem thông tin đơn hàng theo ID"""
    if don_hang_id not in don_hang_db:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn hàng")
    return don_hang_db[don_hang_id]


@app.get("/orders")
def xem_tat_ca_don_hang():
    """Xem tất cả đơn hàng (dành cho admin)"""
    return {"tong_so": len(don_hang_db), "don_hang": list(don_hang_db.values())}


@app.put("/orders/{don_hang_id}/trang-thai")
def cap_nhat_trang_thai(don_hang_id: str, trang_thai_moi: str):
    """
    Cập nhật trạng thái đơn hàng.
    Được gọi bởi các service khác (Payment, Delivery).
    """
    if don_hang_id not in don_hang_db:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn hàng")
    
    cac_trang_thai_hop_le = ["cho_xac_nhan", "dang_chuan_bi", "dang_giao", "da_giao", "da_huy"]
    if trang_thai_moi not in cac_trang_thai_hop_le:
        raise HTTPException(status_code=400, detail=f"Trạng thái không hợp lệ: {trang_thai_moi}")
    
    don_hang_db[don_hang_id]["trang_thai"] = trang_thai_moi
    print(f"[ORDER] Cập nhật đơn {don_hang_id} -> {trang_thai_moi}")
    
    return {"message": "Cập nhật thành công", "trang_thai": trang_thai_moi}


@app.delete("/orders/{don_hang_id}")
def huy_don_hang(don_hang_id: str):
    """Hủy đơn hàng (chỉ được khi còn đang chờ xác nhận)"""
    if don_hang_id not in don_hang_db:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn hàng")
    
    don = don_hang_db[don_hang_id]
    if don["trang_thai"] != "cho_xac_nhan":
        raise HTTPException(
            status_code=400, 
            detail="Chỉ có thể hủy đơn hàng khi đang chờ xác nhận"
        )
    
    don_hang_db[don_hang_id]["trang_thai"] = "da_huy"
    return {"message": "Đã hủy đơn hàng thành công"}
