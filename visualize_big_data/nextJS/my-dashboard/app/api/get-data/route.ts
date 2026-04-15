import { NextResponse } from 'next/server';
import { MongoClient } from 'mongodb';

export async function GET() {
  // Kết nối vào MongoDB trong Docker
  const uri = "mongodb://mongodb:27017"; 
  const client = new MongoClient(uri);

  try {
    await client.connect();
    // THEO ẢNH CỦA MÀY: Tên database là 'analyze'
    const db = client.db("analyze"); 

    // Lấy dữ liệu từ 6 bảng
    const [r1, r2, r3, r4, r5, r6] = await Promise.all([
      db.collection("viz_loan_grade").find({}).toArray(),
      db.collection("viz_loan_scatter").find({}).toArray(),
      db.collection("viz_mkt_job").find({}).toArray(),
      db.collection("viz_mkt_balance").find({}).toArray(),
      db.collection("viz_loan_home").find({}).toArray(),
      db.collection("viz_mkt_edu").find({}).toArray()
    ]);

    await client.close();

    // HÀM CHUYỂN ĐỔI: Ép kiểu số 0/1 thành chuỗi "0.0"/"1.0" để khớp với frontend
    const fmt = (v: any) => parseFloat(v).toFixed(1).toString();

    return NextResponse.json({
      // Biểu đồ 1: Cấu trúc [grade, prediction, count]
      grade: r1.map(i => [i.grade, fmt(i.prediction), i.count]),
      
      // Biểu đồ 2: Cấu trúc [dti, loan_amnt, prediction]
      scatter: r2.map(i => [i.dti, i.loan_amnt, fmt(i.prediction)]),
      
      // Các biểu đồ còn lại
      job: r3.map(i => [i.job, fmt(i.prediction), i.count]),
      balance: r4.map(i => [i.balance_range, fmt(i.prediction), i.count]),
      home: r5.map(i => [i.home_ownership, i.count]),
      edu: r6.map(i => [i.education, i.y, i.count])
    });

  } catch (error: any) {
    console.error("Lỗi kết nối Mongo:", error);
    return NextResponse.json({ error: "Lỗi kết nối database" }, { status: 500 });
  }
}