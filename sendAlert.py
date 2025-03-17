import datetime
import os
from google.cloud import bigquery as bq
from google.cloud.bigquery.job import QueryJobConfig
from google.auth.exceptions import DefaultCredentialsError
from utilEmail import sendEmail
from utilHtml import Json2HtmlTable


class FunctionAlerts:
    def __init__(self, data=None):
        try:
            self.client_bq = bq.Client()
        except DefaultCredentialsError as e:
            print(e)
        self.bq_client = bq.Client()
        self.PROJECT_ID = data['project']
        self.FROM_EMAIL = data['from_email']
        self.TO_EMAIL = data['to_email']
        self.SUBJECT = data['subject']
        self.HTML_CONTENT = data['html_content']
        self.SQL_Daily = data['sql_daily']
        self.SQL_Weekly = data['sql_weely']
        self.SQL_Monthly = data['sql_monthly']

    def publish(self, strHtmltable):
        email = sendEmail(self.PROJECT_ID)
        html_contenttxt = "<html><body><p>{str_content}<br><br>{str_message}<br><br><p>Saludos,</p> <img src='https://www.interseguro.pe/wp-content/uploads/2018/11/logo.png' style='width:265px;height:48px;' /> </body></html>".format(
            str_content=self.HTML_CONTENT, str_message=strHtmltable)
        print(html_contenttxt)
        rptaEmail = email.send(from_email=self.FROM_EMAIL,
                               to_emails=self.TO_EMAIL,
                               subject=self.SUBJECT,
                               html_content=html_contenttxt)
        print(rptaEmail)
        return rptaEmail

    def process(self):
        strHtmltableDaily = self.fetch_report(self.SQL_Daily)
        strHtmltableWeekly = self.fetch_report(self.SQL_Weekly)
        strHtmltableMonthly = self.fetch_report(self.SQL_Monthly)
        fec_proceso_fmt = datetime.datetime.today().strftime('%Y-%m-%d')
        if len(strHtmltableDaily) != 0 or len(strHtmltableWeekly) != 0 or len(strHtmltableMonthly) != 0:
            strHtmltable = """<p><strong>Resumen al: {fec_proceso}</strong></p>
                              <p><strong>Reporte backup diario:</strong></p>
                              <br>{cabecera1}<br>
                              <p><strong>Reporte backup semanal:</strong></p>
                              <br>{cabecera2}<br>
                              <p><strong>Reporte backup mensual:</strong></p>
                              <br>{cabecera3}<br>
                        """.format(fec_proceso=fec_proceso_fmt, cabecera1=strHtmltableDaily, cabecera2=strHtmltableWeekly, cabecera3=strHtmltableMonthly)
            print('##########cuerpo del mensaje##########')
            print(strHtmltable)
            self.publish(strHtmltable)
        else:
            print("Sin datos que reportar.")
            return None
        return strHtmltable

    def fetch_report(self, query):
        try:
            print("query:")
            print(query)
            query_job = self.client_bq.query(query=query, job_config=QueryJobConfig(use_legacy_sql=False))
            table_rows = query_job.result()
            if table_rows is not None:
                records = [dict(row) for row in table_rows]
                json2html = Json2HtmlTable()
                result_html = json2html.convert(records)
            else:
                return None
            return result_html
        except Exception as e:
            print(e)

def function_alerts(data):
    test = FunctionAlerts(data=data)
    test.process()

