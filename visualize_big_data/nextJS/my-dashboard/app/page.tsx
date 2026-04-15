'use client'
import { useEffect, useState } from 'react';
import { Bar, Scatter, Pie } from 'react-chartjs-2';
import { Chart as ChartJS, registerables } from 'chart.js';
import axios from 'axios';

ChartJS.register(...registerables);

export default function Dashboard() {
  const [data, setData] = useState<any>(null);
  const update = async () => { try { const res = await axios.get('/api/get-data'); setData(res.data); } catch (e) {} };
  useEffect(() => { update(); const t = setInterval(update, 5000); return () => clearInterval(t); }, []);

  if (!data || !data.grade || !data.job || !data.balance || !data.home || !data.edu) {
    return <div className="h-screen flex items-center justify-center bg-black text-white text-2xl font-bold animate-pulse">🔄 ĐANG TỔNG HỢP 6 NGUỒN DỮ LIỆU...</div>;
  }

  // --- LOGIC XỬ LÝ DỮ LIỆU AI ---
  const grades = ["A", "B", "C", "D", "E", "F", "G"];
  const stackedGradeData = {
    labels: grades,
    datasets: [
      { label: 'An toàn', data: grades.map(g => Number(data.grade.find((i:any)=>i[0]===g && i[1]==="0.0")?.[2] || 0)), backgroundColor: '#3b82f6' },
      { label: 'Rủi ro', data: grades.map(g => Number(data.grade.find((i:any)=>i[0]===g && i[1]==="1.0")?.[2] || 0)), backgroundColor: '#ef4444' }
    ]
  };

  const collarGroups: any = { "Văn phòng": 0, "Lao động": 0, "Khác": 0 };
  data.job.forEach((i: any) => {
    const job = i[0].toLowerCase(); const count = Number(i[2]);
    if (['admin.', 'management', 'entrepreneur', 'self-employed'].includes(job)) collarGroups["Văn phòng"] += count;
    else if (['blue-collar', 'technician', 'services', 'housemaid'].includes(job)) collarGroups["Lao động"] += count;
    else collarGroups["Khác"] += count;
  });

  const assetGroups: any = { "Đang nợ": 0, "Thấp": 0, "Trung lưu": 0, "VIP": 0 };
  data.balance.forEach((i: any) => {
    const r = Number(i[0]); const c = Number(i[2]);
    if (r < 0) assetGroups["Đang nợ"] += c; else if (r <= 5000) assetGroups["Thấp"] += c; else if (r <= 50000) assetGroups["Trung lưu"] += c; else assetGroups["VIP"] += c;
  });

  return (
    <main className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto">
        <div className="bg-red-700 text-white p-6 rounded-2xl mb-10 shadow-2xl text-center">
            <h1 className="text-4xl font-black uppercase">Data Executive Dashboard - Techcombank</h1>
            <p className="opacity-80">Giám sát Real-time từ Hạ tầng Big Data Cloud Databricks</p>
        </div>

        {/* PHẦN 1: 4 BIỂU ĐỒ AI (PREDICTIVE) */}
        <h2 className="text-2xl font-black text-red-700 mb-6 border-b-4 border-red-500 pb-2">I. PHÂN TÍCH DỰ BÁO TỪ MÔ HÌNH AI</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-16">
          <div className="bg-white p-6 rounded-2xl shadow-lg border-l-8 border-red-600">
            <h3 className="text-black font-bold mb-4 uppercase text-sm">1. Rủi ro nợ xấu theo Grade (D1)</h3>
            <div style={{ height: '250px' }}><Bar data={stackedGradeData} options={{ responsive: true, maintainAspectRatio: false, scales: { x: { stacked: true }, y: { stacked: true } } }} /></div>
          </div>
          <div className="bg-white p-6 rounded-2xl shadow-lg border-l-8 border-blue-600">
            <h3 className="text-black font-bold mb-4 uppercase text-sm">2. Ma trận DTI vs Khoản vay (D1)</h3>
            <div style={{ height: '250px' }}><Scatter options={{ responsive: true, maintainAspectRatio: false }} data={{ datasets: [{ label: 'An toàn', data: data.scatter.filter((i:any)=>i[2]=="0.0").map((i:any)=>({x:i[0], y:i[1]})), backgroundColor: 'blue' }, { label: 'Rủi ro', data: data.scatter.filter((i:any)=>i[2]=="1.0").map((i:any)=>({x:i[0], y:i[1]})), backgroundColor: 'red' }] }} /></div>
          </div>
          <div className="bg-white p-6 rounded-2xl shadow-lg border-l-8 border-yellow-500">
            <h3 className="text-black font-bold mb-4 uppercase text-sm">3. Tiềm năng Marketing theo Nhóm nghề (D2)</h3>
            <div style={{ height: '250px' }} className="flex justify-center"><Pie data={{ labels: Object.keys(collarGroups), datasets: [{ data: Object.values(collarGroups), backgroundColor: ['#10b981', '#f59e0b', '#64748b'] }] }} /></div>
          </div>
          <div className="bg-white p-6 rounded-2xl shadow-lg border-l-8 border-green-600">
            <h3 className="text-black font-bold mb-4 uppercase text-sm">4. Phân loại tài sản khách hàng (D2)</h3>
            <div style={{ height: '250px' }}><Bar data={{ labels: Object.keys(assetGroups), datasets: [{ label: 'Số người', data: Object.values(assetGroups), backgroundColor: '#10b981' }] }} options={{maintainAspectRatio:false}} /></div>
          </div>
        </div>

        {/* PHẦN 2: 2 BIỂU ĐỒ THỐNG KÊ THỰC TẾ (DESCRIPTIVE) */}
        <h2 className="text-2xl font-black text-green-700 mb-6 border-b-4 border-green-500 pb-2">II. THỐNG KÊ DỮ LIỆU GỐC (GROUND TRUTH)</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-10">
          <div className="bg-white p-6 rounded-2xl shadow-lg border-b-8 border-purple-500">
            <h3 className="text-black font-bold mb-4 uppercase text-sm">5. Thực tế: Tình trạng sở hữu nhà ở</h3>
            <div style={{ height: '250px' }} className="flex justify-center"><Pie data={{ labels: data.home.map((i:any)=>i[0]), datasets: [{ data: data.home.map((i:any)=>i[1]), backgroundColor: ['#a855f7','#ec4899','#3b82f6','#10b981'] }] }} /></div>
          </div>
          <div className="bg-white p-6 rounded-2xl shadow-lg border-b-8 border-orange-500">
            <h3 className="text-black font-bold mb-4 uppercase text-sm">6. Thực tế: Chốt đơn theo Học vấn</h3>
            <div style={{ height: '250px' }}><Bar data={{ 
                labels: [...new Set(data.edu.map((i:any)=>i[0]))], 
                datasets: [
                    { label: 'Yes', data: [...new Set(data.edu.map((i:any)=>i[0]))].map(e => Number(data.edu.find((i:any)=>i[0]===e && i[1]==="yes")?.[2] || 0)), backgroundColor: '#f59e0b' },
                    { label: 'No', data: [...new Set(data.edu.map((i:any)=>i[0]))].map(e => Number(data.edu.find((i:any)=>i[0]===e && i[1]==="no")?.[2] || 0)), backgroundColor: '#94a3b8' }
                ]
            }} options={{ responsive: true, maintainAspectRatio: false, scales: { x: { stacked: true }, y: { stacked: true } } }} /></div>
          </div>
        </div>
      </div>
    </main>
  );
}