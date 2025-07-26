from pyspark.sql.functions import col, sum, countDistinct, min, max, to_date, year, month, quarter, when

# --- 1. Khai báo đường dẫn ---
silver_path = "abfss://silver-container@silverngb.dfs.core.windows.net/"
gold_path = "abfss://gold-container@goldngb.dfs.core.windows.net/"

# --- 2. Đọc TOÀN BỘ dữ liệu từ Silver ---
# Logic này sẽ đọc toàn bộ bảng sự kiện và phiên bản HIỆN HÀNH của các bảng chiều.
print("Đang đọc toàn bộ dữ liệu hiện hành từ Silver...")
try:
    # Đọc các bảng chiều và lọc lấy những bản ghi mới nhất (is_current = true)
    dim_customers = spark.read.format("delta").load(f"{silver_path}Customers").where("is_current = true")
    dim_products = spark.read.format("delta").load(f"{silver_path}Products").where("is_current = true")
    dim_sellers = spark.read.format("delta").load(f"{silver_path}Sellers").where("is_current = true")
    dim_categories = spark.read.format("delta").load(f"{silver_path}ProductCategories").where("is_current = true")
    dim_order_status = spark.read.format("delta").load(f"{silver_path}OrderStatus").where("is_current = true")
    
    # Đọc toàn bộ các bảng sự kiện (fact)
    fact_orders = spark.read.format("delta").load(f"{silver_path}Orders")
    fact_order_items = spark.read.format("delta").load(f"{silver_path}OrderItems")
    
    print("✅ Đọc thành công các bảng Silver.")
except Exception as e:
    dbutils.notebook.exit(f"Lỗi khi đọc bảng Silver: {e}")

# --- 3. Tạo bảng fact_order_details lớn từ các bảng Silver ---
# Bước này join các bảng lại với nhau để tạo ra một view phẳng, dễ phân tích
print("Đang join các bảng Silver để tạo bảng fact chính...")
fact_order_details = fact_order_items \
    .join(fact_orders, "OrderID", "inner") \
    .join(dim_customers, "CustomerID", "inner") \
    .join(dim_products, "ProductID", "inner") \
    .join(dim_categories, "CategoryID", "inner") \
    .join(dim_sellers, "SellerID", "inner") \
    .join(dim_order_status, "StatusID", "inner") \
    .select(
        fact_orders["OrderID"], "OrderItemID", "OrderNumber", "CustomerID", dim_customers["Name"].alias("CustomerName"),
        "ProductID", dim_products["Name"].alias("ProductName"), "CategoryID", "CategoryName", "SellerID", dim_sellers["Name"].alias("SellerName"),
        "StatusID", "StatusName", fact_order_items["Quantity"], fact_order_items["CurrentPrice"],
        (fact_order_items["Quantity"] * fact_order_items["CurrentPrice"]).alias("TotalAmount"),
        fact_orders["UpdatedAt"].alias("OrderDate")
    )
# Cache DataFrame này để các bước sau chạy nhanh hơn
fact_order_details.cache()
print("✅ Tạo bảng fact thành công.")

# --- 4. Tạo và GHI ĐÈ (OVERWRITE) các bảng Gold ---

# 4.1. REQUIREMENT 1: Hiệu suất theo người bán, sản phẩm, ngành hàng
print("\n--- Tính toán và ghi đè Gold Table 1: Seller Performance ---")
try:
    df_performance = fact_order_details.withColumn("order_date", to_date(col("OrderDate"))) \
                                      .withColumn("year", year(col("order_date"))) \
                                      .withColumn("quarter", quarter(col("order_date"))) \
                                      .withColumn("month", month(col("order_date")))

    # Bảng Daily
    gold_seller_performance_daily = df_performance.groupBy("SellerID", "SellerName", "ProductID", "ProductName", "CategoryName", "year", "quarter", "month", "order_date") \
                                                  .agg(sum("TotalAmount").alias("total_revenue"), sum("Quantity").alias("total_quantity_sold"), countDistinct("OrderID").alias("distinct_orders"))
    gold_seller_performance_daily.write.format("delta").mode("overwrite").save(f"{gold_path}seller_performance_daily")
    print("    -> ✅ Ghi đè thành công 'seller_performance_daily'")

    # Bảng Monthly
    gold_seller_performance_monthly = gold_seller_performance_daily.groupBy("SellerID", "SellerName", "ProductID", "ProductName", "CategoryName", "year", "month") \
                                                                   .agg(sum("total_revenue").alias("total_revenue"), sum("total_quantity_sold").alias("total_quantity_sold"), sum("distinct_orders").alias("distinct_orders"))
    gold_seller_performance_monthly.write.format("delta").mode("overwrite").save(f"{gold_path}seller_performance_monthly")
    print("    -> ✅ Ghi đè thành công 'seller_performance_monthly'")

    # Bảng Quarterly
    gold_seller_performance_quarterly = gold_seller_performance_daily.groupBy("SellerID", "SellerName", "ProductID", "ProductName", "CategoryName", "year", "quarter") \
                                                                       .agg(sum("total_revenue").alias("total_revenue"), sum("total_quantity_sold").alias("total_quantity_sold"), sum("distinct_orders").alias("distinct_orders"))
    gold_seller_performance_quarterly.write.format("delta").mode("overwrite").save(f"{gold_path}seller_performance_quarterly")
    print("    -> ✅ Ghi đè thành công 'seller_performance_quarterly'")

except Exception as e:
    print(f"❌ Lỗi khi xử lý Requirement 1: {e}")


# 4.2. REQUIREMENT 2: Tỷ lệ hoàn đơn/hủy đơn
print("\n--- Tính toán và ghi đè Gold Table 2: Order Rates ---")
try:
    gold_order_rates = fact_order_details.groupBy("SellerID", "SellerName") \
        .agg(
            countDistinct("OrderID").alias("total_orders_placed"),
            countDistinct(when(col("StatusName") == "Delivered", col("OrderID"))).alias("delivered_orders"),
            countDistinct(when(col("StatusName") == "Cancelled", col("OrderID"))).alias("cancelled_orders"),
            countDistinct(when(col("StatusName") == "Returned", col("OrderID"))).alias("returned_orders")
        ) \
        .withColumn("cancellation_rate", col("cancelled_orders") / col("total_orders_placed")) \
        .withColumn("return_rate", col("returned_orders") / when(col("delivered_orders") > 0, col("delivered_orders")).otherwise(1))
    
    gold_order_rates.write.format("delta").mode("overwrite").save(f"{gold_path}order_rates")
    print("    -> ✅ Ghi đè thành công 'order_rates'")
except Exception as e:
    print(f"❌ Lỗi khi xử lý Requirement 2: {e}")


# 4.3. REQUIREMENT 3: Phân tích nhóm người bán theo phân khúc
print("\n--- Tính toán và ghi đè Gold Table 3: Seller Segmentation ---")
try:
    # Đọc lại dữ liệu tổng hợp vừa được tạo
    df_seller_revenue = spark.read.format("delta").load(f"{gold_path}seller_performance_daily").groupBy("SellerID", "SellerName").agg(sum("total_revenue").alias("total_revenue"))
    df_order_rates = spark.read.format("delta").load(f"{gold_path}order_rates")

    df_seller_kpis = df_seller_revenue.join(df_order_rates.drop("SellerName"), "SellerID", "inner")

    gold_seller_segmentation = df_seller_kpis.withColumn("seller_segment",
        when((col("total_revenue") > 10000) & (col("return_rate") < 0.015), "Top Seller")
        .when((col("total_revenue") > 2000) & (col("return_rate") < 0.03), "Premium Seller")
        .otherwise("Risk Seller")
    )
    gold_seller_segmentation.write.format("delta").mode("overwrite").save(f"{gold_path}seller_segmentation")
    print("    -> ✅ Ghi đè thành công 'seller_segmentation'")
except Exception as e:
    print(f"❌ Lỗi khi tính toán seller_segmentation: {e}")


# 4.4. REQUIREMENT 4: Hiểu rõ hành vi mua của khách hàng
print("\n--- Tính toán và ghi đè Gold Table 4: Customer Analytics ---")
try:
    gold_customer_analytics = fact_order_details.groupBy("CustomerID", "CustomerName") \
                                       .agg(
                                           countDistinct("OrderID").alias("total_orders"),
                                           sum("TotalAmount").alias("total_spend"),
                                           min("OrderDate").alias("first_purchase_date"),
                                           max("OrderDate").alias("last_purchase_date")
                                       ) \
                                       .withColumn("customer_type",
                                           when(col("total_orders") > 1, "Returning Customer")
                                           .otherwise("New Customer")
                                       )

    gold_customer_analytics.write.format("delta").mode("overwrite").save(f"{gold_path}customer_analytics")
    print("    -> ✅ Ghi đè thành công 'customer_analytics'")
except Exception as e:
    print(f"❌ Lỗi khi tính toán customer_analytics: {e}")

# Giải phóng bộ nhớ đã cache
fact_order_details.unpersist()

print("\n--- HOÀN TẤT TOÀN BỘ PIPELINE SILVER TO GOLD ---")
