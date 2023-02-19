from airflow.models.baseoperator import BaseOperator

class NotificationOperator(BaseOperator):

  def __init__(self, type: str,  **kwargs):
    super().__init__(**kwargs)

    self.type = type

  def execute(self, context):
    if (self.type == 'marketing'):
      print(f'Buen día equipo de Marketing,')
      print('La información se encuentra disponible.')
      print('Saludos.')
    elif (self.type == 'analyst'):
      print(f'Hola compañeros Analistas! Ya esta disponible la información! :)')
    else:
      raise Exception("Notificacion para quien?")