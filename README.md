# Solrouter

Router round-robin cho nhiều API key free của fomoapi.io. Các service chỉ cần dùng một router key do Solrouter cấp.

## Chạy local
    npm install
    cp .env.example .env   # đặt ADMIN_PASSWORD
    npm run dev            # dashboard: http://localhost:3000/admin

## Dùng từ service
Đổi base URL `https://api.fomoapi.io` thành URL của router, và dùng router key thay API key
(`authorization: Bearer sr_...`, `x-api-key` hoặc `?key=` đều được).

## Hành vi
- Round-robin giữa các key đang bật. 402 (hết credits) → thử key kế tiếp, key bị đánh dấu `exhausted` và được thử lại sau 30 phút.
  Nếu 402 vẫn báo còn credits (call đắt hơn số dư) thì chỉ thử key khác, không đánh dấu hết quota.
- 401 → key `invalid` (bấm Reset trên dashboard sau khi sửa). 429/502/503/504/lỗi mạng → cooldown 30s.
- Credits còn lại lấy từ header `x-credits-remaining` của mỗi response; nút "Làm mới" gọi `/v2/me`.
- API key upstream mã hoá AES-256-GCM trong SQLite; router key chỉ lưu hash SHA-256.

## Easy Panel
Tạo App từ Dockerfile (hoặc GitHub repo), đặt env `ADMIN_PASSWORD` (và `APP_SECRET` nên đặt cố định),
mount volume vào `/data`, mở cổng 3000.

## Chưa hỗ trợ
WebSocket (`/ws/alerts`, `/ws/trades`) — hiện chỉ proxy HTTP.
