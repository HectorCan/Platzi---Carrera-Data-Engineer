from airflow.models.baseoperator import BaseOperator
'''
  @author Hector Can

  Esto es un operator basico, todavia falta determinar que tipo de uso podria tener
  un operador custom.
'''
class HelloOperator(BaseOperator):
    
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)

        self.name = name
    
    def execute(self, context):
        print(f'Hola {self.name}')