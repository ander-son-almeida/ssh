import paramiko
import pandas as pd
import io
import os

# Configurações da conexão SSH
hostname = 'seu_servidor.com'  # ou IP do servidor
port = 22  # porta padrão SSH
username = 'seu_usuario'
password = 'sua_senha'  # ou use chave SSH

# Nome do arquivo Python remoto que será executado
remote_script_name = 'gerar_dataframe.py'
# Nome do arquivo CSV temporário que será gerado
remote_csv_name = 'dataframe_temp.csv'

# Script Python que será enviado para o servidor remoto
python_script = f"""
import pandas as pd
import numpy as np

# Gerar um DataFrame de exemplo
df = pd.DataFrame({{
    'Coluna_A': np.random.rand(10),
    'Coluna_B': np.random.randint(0, 100, size=10),
    'Coluna_C': ['Texto_' + str(i) for i in range(10)]
}})

# Salvar o DataFrame em um arquivo CSV temporário
df.to_csv('{remote_csv_name}', index=False)
print(f"DataFrame gerado e salvo em {{'{remote_csv_name}'}}")
"""

def executar_remoto_e_obter_df():
    # Criar conexão SSH
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # Conectar ao servidor
        ssh.connect(hostname, port, username, password)
        print("Conexão SSH estabelecida com sucesso!")
        
        # Criar uma sessão SFTP para transferir arquivos
        sftp = ssh.open_sftp()
        
        # Enviar o script Python para o servidor remoto
        with sftp.file(remote_script_name, 'w') as remote_file:
            remote_file.write(python_script)
        print(f"Script {remote_script_name} enviado para o servidor remoto.")
        
        # Executar o script remoto
        stdin, stdout, stderr = ssh.exec_command(f'python3 {remote_script_name}')
        
        # Verificar erros
        errors = stderr.read().decode()
        if errors:
            print("Erros durante a execução remota:")
            print(errors)
            return None
        
        print("Script executado com sucesso no servidor remoto.")
        print(stdout.read().decode())
        
        # Baixar o arquivo CSV gerado
        with sftp.file(remote_csv_name, 'r') as remote_file:
            csv_content = remote_file.read().decode()
        
        # Ler o CSV para um DataFrame pandas local
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Remover arquivos temporários no servidor remoto
        ssh.exec_command(f'rm {remote_script_name} {remote_csv_name}')
        print("Arquivos temporários removidos do servidor remoto.")
        
        return df
    
    except Exception as e:
        print(f"Erro durante a operação SSH: {str(e)}")
        return None
    
    finally:
        # Fechar conexões
        if 'sftp' in locals():
            sftp.close()
        ssh.close()
        print("Conexão SSH fechada.")

# Executar a função principal
df_remoto = executar_remoto_e_obter_df()

if df_remoto is not None:
    print("\nDataFrame recebido do servidor remoto:")
    print(df_remoto)
else:
    print("Falha ao obter o DataFrame do servidor remoto.")
