import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException


class sendEmail:
    def __init__(self, project_id):
        print(project_id)

    def send(self, from_email, to_emails, subject, html_content):
        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = os.environ.get('SENDINBLUE_API_KEY')
        if not configuration.api_key['api-key']:
            raise ValueError("SENDINBLUE_API_KEY environment variable is not set")
        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
        print('@@@@@@@@@@@@@@@@@@@')
        print(from_email)
        print(to_emails)
        print('@@@@@@@@@@@@@@@@@@@')
        #from_email = {"name":"Sendinblue","email":"comunicaciones@interseguro.com.pe"}
        #to_emails = [{"email":"jhuertad@gmail.com","name":"John Huerta"}]

        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to_emails, html_content=html_content, sender=from_email, subject=subject)
        try:
            api_response = api_instance.send_transac_email(send_smtp_email)
            print(str(api_response))
        except ApiException as e:
            print("Excepción cuando se llama al SMTPApi->send_transac_email:"+str(e))