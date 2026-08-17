# Spark & Kafka Summary Project

מימוש מלא לתרגיל הסיכום המצורף תחת `docs/`.

## החלטות לגבי הפערים במסמך

- התרשים `docs/Project.png` הוא מקור האמת לארכיטקטורה: אין שלב `Anomaly Detection`, ולכן `AlertCounter` קורא רק מה-topic בשם `alert-data`.
- אירוע נחשב להתראה כאשר לפחות אחד משלושת התנאים מתקיים (`OR`).
- `Gray`, `Grey` ו-`Silver` נספרים כולם תחת `num_of_silver`.
- "15 הדקות האחרונות" ממומש כחלון מתגלגל של 15 דקות המתעדכן בכל דקה.
- ה-`expected_gear` מחושב בדיוק לפי הדרישה: `round(speed / 30)`.
- כל job משתמש כברירת מחדל ב-`local[1]`; יישומי ה-streaming מקבלים 512MB. זה מספיק לקצב התרגיל ומאפשר לכל ארבעת התהליכים לרוץ יחד. ניתן לשנות זאת עם `SPARK_MASTER` ו-`SPARK_DRIVER_MEMORY`.
- רמת הלוג מוגדרת כברירת מחדל ל-`WARN`, כדי שטבלאות הקונסול לא ייעלמו בתוך הודעות `INFO`. ניתן לשנות זאת עם `SPARK_LOG_LEVEL`.

## מבנה ה-pipeline

1. `model_creation.py` יוצר את `s3a://spark/data/dims/car_models`.
2. `color_creation.py` יוצר את `s3a://spark/data/dims/car_colors`.
3. `cars_generator.py` יוצר 20 מכוניות ושומר אותן ב-`s3a://spark/data/dims/cars`.
4. `data_generator.py` שולח אירוע לכל מכונית בכל שנייה אל `sensors-sample`.
5. `data_enrichment.py` מעשיר את האירועים ושולח אותם אל `samples-enriched`.
6. `alert_detection.py` מסנן אירועי התראה ושולח אותם אל `alert-data`.
7. `alert_counter.py` מדפיס לקונסול את מדדי חלון 15 הדקות.

## הרצה בסביבת הקורס

מהמחשב המארח, מעתיקים את התיקייה לקונטיינר:

```powershell
docker cp .\spark\spark_kafka_summary_project dev_env:/root/spark_kafka_summary_project
docker exec -it dev_env bash
```

בתוך `dev_env`, מכינים bucket, topics וטבלאות dimension:

```bash
cd /root/spark_kafka_summary_project
bash scripts/run_setup.sh
```

לאחר מכן פותחים ארבעה terminals בתוך `dev_env`, ובכל אחד מריצים פקודה אחת. מומלץ להפעיל לפי הסדר הבא:

```bash
cd /root/spark_kafka_summary_project
bash scripts/spark_stream.sh src/data_enrichment.py
```

```bash
cd /root/spark_kafka_summary_project
bash scripts/spark_stream.sh src/alert_detection.py
```

```bash
cd /root/spark_kafka_summary_project
bash scripts/spark_stream.sh src/alert_counter.py
```

```bash
cd /root/spark_kafka_summary_project
python3 src/data_generator.py
```

בפעם הראשונה שלושת יישומי ה-streaming יורידו את מחבר Kafka המתאים ל-Spark 3.4.0. אפשר לשנות כל כתובת, topic או נתיב באמצעות משתני הסביבה שב-`.env.example`.

## בדיקות מהירות

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q src scripts tests
```

Kafdrop זמין במחשב המארח ב-`http://localhost:9003`, ו-MinIO Console ב-`http://localhost:9002`.
