from fpdf import FPDF

def add_part(pdf, part, w, h):
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, f'Part {part}', border = 0, ln = 1)
	pdf.ln()

def add_plot(pdf, plot, title, description, w, h):
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, title, ln=1)
	pdf.set_font('Times', size=12)
	pdf.cell(w, h, description, ln=1)
	pdf.ln()
	pdf.image(plot, x=50, w=pdf.w/2.0)

def output_df_to_pdf(pdf, df):
	table_cell_width = 27.5
	table_cell_height = 6
	pdf.set_font('Arial', 'B', 8)

	for col in df.columns:
		pdf.cell(table_cell_width, table_cell_height, str(col), align='C', border=1)
	pdf.ln(table_cell_height)
	pdf.set_font('Arial', '', 8)
	for _, row in df.iterrows():
		for col in df.columns:
			value = str(row[col])
			pdf.cell(table_cell_width, table_cell_height, value, align='C', border=1)
		pdf.ln(table_cell_height)

def split_table(table, axis=0, chunk_size=10):
	for i in range(0, table.shape[axis], chunk_size):
		if axis == 0:
			yield table[i:i + chunk_size]
		else:
			yield table.iloc[:, i:i + chunk_size]

def add_table(pdf, table, title, description, w, h):
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, title, border = 0, ln = 1)
	pdf.set_font('Times', size=12)
	pdf.cell(w, h, description, border = 0, ln = 1)
	output_df_to_pdf(pdf, table)
	pdf.ln()

def split_text(text, chunk_size=12):
	text = text.split()
	for i in range(0, len(text), chunk_size):
		yield ' '.join(text[i:i + chunk_size])
