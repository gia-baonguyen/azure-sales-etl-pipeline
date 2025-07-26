from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, max as _max, when
from delta.tables import DeltaTable

# --- 1. Khai báo đường dẫn GỐC và Database ---
bronze_base_path = "abfss://bronze-container@bronzengb.dfs.core.windows.net/"
silver_base_path = "abfss://silver-container@silverngb.dfs.core.windows.net/"
silver_watermark_path = f"{silver_base_path}_watermarks/"
silver_db_name = "silver"

# --- 2. Tạo Database trong Metastore nếu chưa tồn tại ---
spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db_name}")
print(f"Database '{silver_db_name}' đã sẵn sàng.")


# --- 3. Bảng điều khiển Metadata ---
tables_metadata = {
    "Customers":         {"pk": "CustomerID"},
    "Products":          {"pk": "ProductID"},
    "Sellers":           {"pk": "SellerID"},
    "Addresses":         {"pk": "AddressID"},
    "ProductCategories": {"pk": "CategoryID"},
    "OrderStatus":       {"pk": "StatusID"},
    "PaymentMethods":    {"pk": "PaymentMethodID"},
    "Orders":            {"pk": "OrderID"},
    "OrderItems":        {"pk": "OrderItemID"},
    "Payments":          {"pk": "PaymentID"},
    "Reviews":           {"pk": "ReviewID"},
    "Reasons":           {"pk": "ReasonID"},
    "ShoppingCarts":     {"pk": "CartID"},
    "CartItems":         {"pk": "CartItemID"},
    "Inventory":         {"pk": "InventoryID"}
}

# --- 4. Vòng lặp chính để xử lý tất cả các bảng ---
for table_name, config in tables_metadata.items():
    print(f"\n--- Bắt đầu xử lý SCD Type 2 cho bảng: {table_name} ---")
    
    source_table_path = f"{bronze_base_path}{table_name}"
    silver_table_path = f"{silver_base_path}{table_name}"
    pk_column = config["pk"]
    watermark_file_path = f"{silver_watermark_path}{table_name}"
    
    try:
        last_processed_timestamp = '1900-01-01T00:00:00.000+0000'
        try:
            last_processed_timestamp = spark.read.text(watermark_file_path).first()[0]
            print(f"    -> Watermark lần trước: {last_processed_timestamp}")
        except Exception as e:
            if "Path does not exist" in str(e):
                print("    -> Không tìm thấy file watermark, sẽ xử lý toàn bộ dữ liệu Bronze.")
            else:
                raise e

        # Chỉ đọc những dòng mới từ Bronze dựa trên watermark
        source_df = spark.read.format("delta").load(source_table_path) \
                           .where(col("ingestion_timestamp") > last_processed_timestamp)
        
        source_df.cache()
        new_rows_count = source_df.count()
        print(f"    -> Đã đọc {new_rows_count} dòng mới từ Bronze (sau khi lọc bằng watermark).")
        
        if new_rows_count == 0:
            print(f"    -> Không có thay đổi mới kể từ lần chạy cuối. Bỏ qua.")
            source_df.unpersist()
            continue
        
        new_watermark = source_df.select(_max("ingestion_timestamp")).first()[0]
        
        cols_to_exclude = {pk_column, "ingestion_timestamp", "source_file", "source_path", "CreatedAt", "UpdatedAt"}
        tracked_cols = [c for c in source_df.columns if c not in cols_to_exclude]
        
        source_df_with_hash = source_df.withColumn("hash", sha2(concat_ws("||", *tracked_cols), 256))
        
        is_target_delta_table = DeltaTable.isDeltaTable(spark, silver_table_path)

        if not is_target_delta_table:
            print(f"    -> Bảng đích chưa tồn tại. Sẽ tạo mới với các cột SCD.")
            source_df_with_hash.drop("hash").withColumn("is_current", lit(True)) \
                               .withColumn("effective_date", current_timestamp()) \
                               .withColumn("end_date", lit(None).cast("timestamp")) \
                               .write.format("delta").save(silver_table_path)
        else:
            target_table = DeltaTable.forPath(spark, silver_table_path)
            target_df_with_hash = target_table.toDF().where("is_current = true").withColumn("hash", sha2(concat_ws("||", *tracked_cols), 256))
            
            # Dùng FULL OUTER JOIN để so sánh nguồn và đích
            join_df = source_df_with_hash.alias("s").join(
                target_df_with_hash.alias("t"),
                pk_column,
                "full_outer"
            )

            # Xác định các hành động cần thực hiện
            staged_df = join_df.withColumn("action", 
                when(col(f"t.{pk_column}").isNull(), "INSERT")
                .when(col(f"s.{pk_column}").isNull(), "NO CHANGE")
                .when(col("s.hash") != col("t.hash"), "UPDATE")
                .otherwise("NO CHANGE")
            ).where("action IN ('INSERT', 'UPDATE')")

            if staged_df.rdd.isEmpty():
                 print("    -> Không có thay đổi thuộc tính cần cập nhật SCD.")
            else:
                print(f"    -> Tìm thấy {staged_df.count()} bản ghi mới hoặc đã thay đổi.")
                staged_df.cache() # Cache để tái sử dụng
                
                # ----- THÊM LẠI LOGIC TÌM VÀ LƯU KHÁCH HÀNG MỚI -----
                if table_name == "Customers":
                    print("    -> Bảng Customers, đang kiểm tra khách hàng mới để gửi email...")
                    # Lọc ra những khách hàng có hành động là INSERT
                    df_new_customers = staged_df.where("action = 'INSERT'").select("s.Name", "s.Email")
                    
                    if not df_new_customers.rdd.isEmpty():
                        temp_table_name = "default.new_customers_for_emailing"
                        print(f"    -> Tìm thấy {df_new_customers.count()} khách hàng mới. Đang đăng ký bảng tạm...")
                        df_new_customers.write.format("delta").mode("overwrite").saveAsTable(temp_table_name)
                        print(f"    -> Đã tạo/cập nhật bảng '{temp_table_name}' trong metastore.")
                    else:
                        print("    -> Không tìm thấy khách hàng mới trong lần chạy này.")
                # ----- KẾT THÚC PHẦN LOGIC TÌM KHÁCH HÀNG MỚI -----

                new_records_to_insert = staged_df.select("s.*")
                keys_to_expire = staged_df.where("action = 'UPDATE'").select(f"s.{pk_column}").distinct()

                if not keys_to_expire.rdd.isEmpty():
                    print("    -> Đóng các bản ghi cũ...")
                    target_table.alias("t").merge(
                        keys_to_expire.alias("s"),
                        f"t.{pk_column} = s.{pk_column}"
                    ).whenMatchedUpdate(
                        condition="t.is_current = true",
                        set={"is_current": "false", "end_date": "current_timestamp()"}
                    ).execute()

                print("    -> Chèn các bản ghi mới/đã cập nhật...")
                final_inserts = new_records_to_insert.drop("hash").withColumn("is_current", lit(True)) \
                                                      .withColumn("effective_date", current_timestamp()) \
                                                      .withColumn("end_date", lit(None).cast("timestamp"))
                
                final_inserts.write.format("delta").mode("append").save(silver_table_path)
                staged_df.unpersist() # Giải phóng bộ nhớ

        # Cập nhật watermark
        if new_watermark:
            spark.createDataFrame([(str(new_watermark),)], ["timestamp"]) \
                 .write.mode("overwrite").text(watermark_file_path)
            print(f"    -> Cập nhật watermark mới thành công: {new_watermark}")

        print(f"    -> Hoàn tất SCD Type 2 cho bảng {table_name}.")
        source_df.unpersist()

    except Exception as e:
        if "Path does not exist" in str(e) or "PATH_NOT_FOUND" in str(e):
            print(f"    -> BỎ QUA: Thư mục nguồn không tồn tại: {source_table_path}")
            continue
        else:
            print(f"❌ Lỗi khi xử lý bảng {table_name}: {e}")
            raise e

print("\n--- Hoàn tất xử lý tất cả các bảng từ Bronze sang Silver! ---")
