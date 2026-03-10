import ExcelJS from 'exceljs';

export const parseExcelBuffer = async (buffer: Buffer) => {
    const workbook = new ExcelJS.Workbook();
    // ExcelJS accepts Buffer
    await workbook.xlsx.load(buffer as any);
    const worksheet = workbook.worksheets[0];

    const data: any[] = [];
    const headers: string[] = [];

    worksheet.eachRow((row: ExcelJS.Row, rowNumber: number) => {
        const values = row.values as any[];
        if (rowNumber === 1) {
            for (let i = 1; i < values.length; i++) {
                headers.push(String(values[i] ?? '').trim());
            }
        } else {
            const obj: any = {};
            for (let i = 1; i < values.length; i++) {
                obj[headers[i - 1] || `col_${i}`] = values[i];
            }
            data.push(obj);
        }
    });

    return data;
};
