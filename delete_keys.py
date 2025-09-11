import redis

# Cấu hình kết nối
r = redis.Redis(
    host='localhost',      # thay bằng host của bạn
    port=6379,
    db=0,
    password=None          # thêm password nếu có
)

pattern = "call*"
cursor = 0
total_deleted = 0

print(f"Đang quét và xóa tất cả key khớp mẫu: {pattern}")

while True:
    cursor, keys = r.scan(cursor=cursor, match=pattern, count=100)
    if keys:
        deleted = r.delete(*keys)
        total_deleted += deleted
        print(f"Đã xóa {deleted} key: {keys}")

    if cursor == 0:
        break

print(f"\n✅ Tổng cộng đã xóa {total_deleted} key.")