from pyspark.sql.functions import (
    col,
    dayofweek,
    dayofmonth,
    avg
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# ---------------------------------------------------------
# 1. Leitura dos dados da camada Gold
# ---------------------------------------------------------
df = spark.table("gold_revenue_by_day")

# ---------------------------------------------------------
# 2. Feature Engineering (simples e explicável)
# ---------------------------------------------------------
df_features = (
    df
    .withColumn("day_of_week", dayofweek(col("order_date")))
    .withColumn("day_of_month", dayofmonth(col("order_date")))
    .select(
        "day_of_week",
        "day_of_month",
        col("total_sales").alias("label")
    )
)

# ---------------------------------------------------------
# 3. Split treino / teste
# ---------------------------------------------------------
train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

# ---------------------------------------------------------
# 4. BASELINE: prever sempre a média do treino
# ---------------------------------------------------------
baseline_mean = train_df.select(avg("label")).collect()[0][0]

baseline_predictions = (
    test_df
    .withColumn("prediction", col("label") * 0 + baseline_mean)
)

evaluator = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="rmse"
)

baseline_rmse = evaluator.evaluate(baseline_predictions)

print(f"Baseline RMSE (média): {baseline_rmse:.2f}")

# ---------------------------------------------------------
# 5. Preparação das features para o modelo
# ---------------------------------------------------------
assembler = VectorAssembler(
    inputCols=["day_of_week", "day_of_month"],
    outputCol="features"
)

train = assembler.transform(train_df).select("features", "label")
test = assembler.transform(test_df).select("features", "label")

# ---------------------------------------------------------
# 6. Treinamento do modelo de Regressão Linear
# ---------------------------------------------------------
lr = LinearRegression()
model = lr.fit(train)

# ---------------------------------------------------------
# 7. Avaliação do modelo
# ---------------------------------------------------------
predictions = model.transform(test)

model_rmse = evaluator.evaluate(predictions)

print(f"Model RMSE (Linear Regression): {model_rmse:.2f}")

# ---------------------------------------------------------
# 8. Comparação final
# ---------------------------------------------------------
improvement = baseline_rmse - model_rmse

print("--------------------------------------------------")
print(f"Melhoria em relação ao baseline: {improvement:.2f}")
print("--------------------------------------------------")