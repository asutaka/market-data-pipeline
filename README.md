# Market Data Pipeline (.NET 8 + Kafka + ClickHouse + ArgoCD)

Hệ thống xử lý luồng dữ liệu chứng khoán thời gian thực:
- **Producer**: Bắn tick giá các mã cổ phiếu VN30 (FPT, HPG, VCB, SSI...) vào Apache Kafka topic `market-ticks`.
- **Consumer**: Đọc tick từ Kafka, tổng hợp nến và lưu trữ trực tiếp vào ClickHouse.
- **GitOps**: Tự động build Docker image bằng GitHub Actions và deploy lên Kubernetes qua ArgoCD.
