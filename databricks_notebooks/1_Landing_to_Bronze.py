from pyspark.sql.functions import current_timestamp, input_file_name
from delta.tables import DeltaTable

# --- 1. Khai báo đường dẫn gốc ---
landing_path = "abfss://landing-container@landingngb.dfs.core.windows.net/"
bronze_path = "abfss://bronze-container@bronzengb.dfs.core.windows.net/"

print("--- Bắt đầu xử lý tất cả các bảng từ Landing sang Bronze ---")

# --- 2. Lấy danh sách tất cả các file/thư mục trong Landing Zone ---
try:
    all_landing_items = dbutils.fs.ls(landing_path)
except Exception as e:
    dbutils.notebook.exit(f"Lỗi khi truy cập vào Landing Zone: {e}")

# --- 3. Vòng lặp để xử lý từng mục ---
for item in all_landing_items:
    item_name = item.name.strip('/')
    
    if item_name.startswith('_'):
        continue

    # Luôn loại bỏ phần mở rộng .csv để lấy tên bảng chuẩn
    table_name = item_name.replace('.csv', '')
    
    print(f"\n--- Bắt đầu xử lý cho bảng: {table_name} (từ nguồn: {item_name}) ---")

    source_path = item.path
    target_path = f"{bronze_path}{table_name}/"

    try:
        # ----- LOGIC MỚI: TÁI CẤU TRÚC ĐỂ AN TOÀN HƠN -----
        
        # Bước A: Đọc dữ liệu từ nguồn CSV
        df_new_data = None
        is_bronze_delta_table = DeltaTable.isDeltaTable(spark, target_path)
        read_options = {"header": "true"}
        
        if is_bronze_delta_table:
            print(f"    -> Bảng Bronze đã tồn tại. Đọc CSV với schema hiện có.")
            bronze_schema = spark.read.format("delta").load(target_path).schema
            df_new_data = spark.read.format("csv").schema(bronze_schema).options(**read_options).load(source_path)
        else:
            print(f"    -> Bảng Bronze chưa tồn tại. Sẽ tự động đoán schema.")
            read_options["inferSchema"] = "true"
            df_new_data = spark.read.format("csv").options(**read_options).load(source_path)
        
        # Bước B: Kiểm tra xem có dữ liệu để xử lý không
        if df_new_data is None or df_new_data.rdd.isEmpty():
            print(f"    -> Không có dữ liệu mới trong {item_name}. Bỏ qua.")
            continue
            
        # Bước C: Thêm metadata và Ghi vào Bronze
        df_with_metadata = df_new_data.withColumn("ingestion_timestamp", current_timestamp()) \
                                      .withColumn("source_file", input_file_name())

        if is_bronze_delta_table:
            # Nếu bảng đã tồn tại, ghi thêm (append)
            df_with_metadata.write.format("delta") \
                               .option("mergeSchema", "true") \
                               .mode("append") \
                               .save(target_path)
        else:
            # Nếu bảng chưa tồn tại, tạo mới
            df_with_metadata.write.format("delta") \
                               .option("mergeSchema", "true") \
                               .save(target_path)

        print(f"✅ Ghi thành công các thay đổi của bảng '{table_name}' vào Bronze.")

    except Exception as e:
        print(f"❌ Lỗi khi xử lý bảng {table_name}: {e}")
        continue

print("\n--- Hoàn tất toàn bộ quá trình Landing to Bronze ---")
