import sys
import boto3
import pandas as pd
from io import StringIO


#Extraccion
s3_bucket = 'mramosotrabajo1'
ruta_del_archivo_1 = 'https://datosabiertos.metropol.gov.co/sites/default/files/uploaded_resources/AnalisisRiesgo_20082017.csv'
ruta_del_archivo_2 = 'https://datosabiertos.metropol.gov.co/sites/default/files/uploaded_resources/TasaMortalidad_2008-2017.csv'
ruta_del_archivo_3 = 'https://datosabiertos.metropol.gov.co/sites/default/files/uploaded_resources/DefunsionesCalculadas_2008-2017.csv'

datos_1 = pd.read_csv(ruta_del_archivo_1,header=0,delimiter=';')
datos_2 = pd.read_csv(ruta_del_archivo_2,header=0,delimiter=';')
datos_3 = pd.read_csv(ruta_del_archivo_3,header=0,delimiter=';')

#Crea tablas en memoria
csv_buffer_1 = StringIO() 
datos_1.to_csv(csv_buffer_1, index=False) 

csv_buffer_2 = StringIO() 
datos_2.to_csv(csv_buffer_2, index=False) 

csv_buffer_3 = StringIO() 
datos_3.to_csv(csv_buffer_3, index=False) 


#Conexion boto3
aws_access_key_id='ASIA3FLD5A3KTPWYG4ZI'
aws_secret_access_key='uoMYv1mPNUmlVJcBnXryDK+hvnsPfVKsjZJ/b9kJ'
aws_session_token='FwoGZXIvYXdzEAsaDDm9zuisrTpl9iKvDiLFAZTsvZLy8VL9VHXfkGuLi7R+onKFvvMt+CtGMTPc2R8Qn/mQxzUzA/DbmQxb4Mn+8uPk2V7HumllQCioAj/5ogOlzwL2S46mnLn37lGOV4SNizexj18VCJSZmWdY/kpyFmiHxUpkPgl6cqWoZrVPLgPUmOtBcijDIaw6klzs5g+y88/xyb9+eXqng8+X6m6jTylDLmqhi43b/TROCZHcAkY+gDY5Z7OwwX/uSPc+tOzHXqRYDMEWsS90LL99pRGRgb13OHnRKMCysq8GMi1P79C1tGQagMpdFVJFwS0c12/SPcMq0Xq5uDXgrYvzVFLfHc6HAPy6XWEGEBo='
session = boto3.Session(aws_access_key_id,aws_secret_access_key,aws_session_token)
s3=session.client('s3')


#Ingesta
s3.put_object(Body=csv_buffer_1.getvalue(),Bucket=s3_bucket, Key= 'dataCVS/Analisis_riesgo/Analisis_Riesgo.csv')
s3.put_object(Body=csv_buffer_2.getvalue(),Bucket=s3_bucket, Key= 'dataCVS/Tasa_Mortalidad/Tasa_Mortalidad.csv')
s3.put_object(Body=csv_buffer_3.getvalue(),Bucket=s3_bucket, Key= 'dataCVS/Defunciones/Defunciones_Calculadas.csv')