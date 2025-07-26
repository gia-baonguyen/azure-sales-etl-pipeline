import pyodbc
import sys
import os
import re

# --- CẤU HÌNH KẾT NỐI ---
# Script sẽ kết nối đến CSDL 'master' để có quyền tạo CSDL mới.
SERVER_NAME = 'localhost,1434'
DATABASE_TO_CREATE = 'testdb'
USER = 'sa'
PASSWORD = 'yourStrong(!)Password'
SQL_SCRIPT_FILE = 'setup_database.sql'

def setup_database():
    """
    Connects to the SQL Server, reads the setup script, and executes it
    to create and configure the database.
    """
    # Kiểm tra xem tệp .sql có tồn tại không
    if not os.path.exists(SQL_SCRIPT_FILE):
        print(f"❌ Lỗi: Không tìm thấy tệp '{SQL_SCRIPT_FILE}'.")
        print("Vui lòng đảm bảo bạn đã lưu tệp SQL vào cùng thư mục với kịch bản Python này.")
        return

    print("--- [BẮT ĐẦU QUÁ TRÌNH THIẾT LẬP CƠ SỞ DỮ LIỆU] ---")

    # Đọc toàn bộ nội dung của tệp .sql
    try:
        with open(SQL_SCRIPT_FILE, 'r', encoding='utf-8') as f:
            full_sql_script = f.read()
        print(f"✅ Đã đọc thành công tệp '{SQL_SCRIPT_FILE}'.")
    except Exception as e:
        print(f"❌ Lỗi khi đọc tệp SQL: {e}")
        return

    # Tách kịch bản SQL thành các khối lệnh dựa trên từ khóa 'GO'
    # GO không phải là lệnh T-SQL, nó là dấu phân tách khối lệnh mà pyodbc không hiểu.
    # Chúng ta phải tự tách nó ra.
    sql_batches = re.split(r'\s+GO\s+', full_sql_script, flags=re.IGNORECASE)
    sql_batches = [batch for batch in sql_batches if batch.strip()] # Lọc bỏ các khối rỗng

    if not sql_batches:
        print("❌ Kịch bản SQL không chứa lệnh nào có thể thực thi.")
        return

    conn = None
    try:
        # Bước 1: Kết nối đến CSDL 'master' để tạo CSDL mới
        # autocommit=True là cần thiết để các lệnh DDL (CREATE DATABASE/TABLE) được thực thi ngay lập tức.
        print(f"🔌 Đang kết nối tới máy chủ '{SERVER_NAME}'...")
        master_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SERVER_NAME};"
            f"DATABASE=master;"  # Kết nối tới master trước
            f"UID={USER};"
            f"PWD={PASSWORD};"
            f"TrustServerCertificate=yes;" # Thêm dòng này nếu bạn dùng chứng chỉ tự ký
        )
        conn = pyodbc.connect(master_conn_str, autocommit=True)
        cursor = conn.cursor()
        print("✅ Kết nối tới 'master' thành công.")

        # Bước 2: Thực thi từng khối lệnh một
        print(f"\n▶️ Chuẩn bị thực thi {len(sql_batches)} khối lệnh...")
        for i, batch in enumerate(sql_batches):
            print(f"   -> Đang thực thi khối {i + 1}/{len(sql_batches)}...")
            cursor.execute(batch)
        
        print("\n🎉 THÀNH CÔNG! Cơ sở dữ liệu và các bảng đã được tạo thành công.")
        print("Bây giờ bạn có thể chạy kịch bản 'truncate' hoặc 'data_seeder'.")

    except pyodbc.Error as e:
        # Lấy ra thông báo lỗi cụ thể từ SQL Server
        sqlstate = e.args[0]
        print(f"❌ Lỗi PYODBC: {sqlstate}")
        print(f"   -> Thông báo lỗi: {e}")

    except Exception as e:
        print(f"❌ Đã xảy ra lỗi không xác định: {e}")

    finally:
        # Luôn đảm bảo kết nối được đóng lại
        if conn:
            conn.close()
            print("\n🔌 Đã đóng kết nối tới cơ sở dữ liệu.")

if __name__ == '__main__':
    setup_database()