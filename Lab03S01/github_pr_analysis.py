import requests
import csv
from datetime import datetime
import time
import sys

# Configuração
GITHUB_TOKEN = "SEU_TOKEN_AQUI"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}
BASE_URL = "https://api.github.com"
CSV_FILE = "github_pr_data.csv"
REPOS_LIMIT = 200  # Limite original de repositórios
PRS_LIMIT = 30  # Limite original de PRs por repositório
MAX_RETRIES = 3  # Número máximo de tentativas para cada requisição

def buscar_repositorios_populares(quantidade=REPOS_LIMIT):
    """
    Busca os repositórios mais populares do GitHub.
    """
    print(f"Buscando os {quantidade} repositórios mais populares...")
    repos = []
    page = 1
    while len(repos) < quantidade:
        url = f"{BASE_URL}/search/repositories?q=stars:>1&sort=stars&order=desc&per_page=100&page={page}"
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            repos.extend(data["items"])
            print(f"Encontrados {len(repos)} repositórios até agora.")
            page += 1
        except requests.RequestException as e:
            print(f"Erro ao buscar repositórios: {e}")
            break
    return repos[:quantidade]

def coletar_dados_pr(repo_name, pr):
    """
    Coleta dados de um pull request específico.
    """
    pr_number = pr["number"]
    pr_url = pr["url"]
    
    try:
        pr_response = requests.get(pr_url, headers=HEADERS)
        pr_response.raise_for_status()
        pr_data = pr_response.json()
        
        reviews_url = f"{pr_url}/reviews"
        reviews_response = requests.get(reviews_url, headers=HEADERS)
        reviews_response.raise_for_status()
        reviews = reviews_response.json()
        
        if len(reviews) == 0:
            return None
        
        created_at = datetime.strptime(pr_data["created_at"], "%Y-%m-%dT%H:%M:%SZ")
        closed_at = datetime.strptime(pr_data["closed_at"], "%Y-%m-%dT%H:%M:%SZ")
        review_time = (closed_at - created_at).total_seconds() / 3600
        
        if review_time < 1:
            return None
        
        # Coletar os logins dos participantes de forma segura
        participants = set()
        if pr_data.get("user") and pr_data["user"].get("login"):
            participants.add(pr_data["user"]["login"])
        for review in reviews:
            if review.get("user") and review["user"].get("login"):
                participants.add(review["user"]["login"])
        
        return {
            "repo_name": repo_name,
            "pr_number": pr_number,
            "num_files_changed": pr_data["changed_files"],
            "lines_added": pr_data["additions"],
            "lines_removed": pr_data["deletions"],
            "review_time_in_hours": review_time,
            "pr_description_length": len(pr_data.get("body") or ""),
            "num_comments": pr_data["comments"],
            "num_participants": len(participants)
        }
    except requests.RequestException as e:
        print(f"Erro ao coletar dados do PR {pr_number} do repositório {repo_name}: {e}")
        return None
    except KeyError as e:
        print(f"Erro ao acessar dados do PR {pr_number} do repositório {repo_name}: Chave {e} não encontrada")
        return None

def fazer_requisicao_com_retry(url, headers, max_retries=MAX_RETRIES):
    """
    Faz uma requisição com retry em caso de erro 403.
    """
    for tentativa in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                reset_time = int(e.response.headers.get('X-RateLimit-Reset', 0))
                sleep_time = max(reset_time - time.time(), 0) + 1
                print(f"Limite de taxa atingido. Aguardando {sleep_time:.2f} segundos.")
                time.sleep(sleep_time)
            else:
                raise
    raise Exception(f"Falha após {max_retries} tentativas")

def main():
    """
    Função principal que coordena a coleta de dados dos PRs e a escrita no arquivo CSV.
    """
    repos = buscar_repositorios_populares()
    
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["repo_name", "pr_number", "num_files_changed", "lines_added", "lines_removed", 
                      "review_time_in_hours", "pr_description_length", "num_comments", "num_participants"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for repo_index, repo in enumerate(repos, 1):
            repo_name = repo["full_name"]
            print(f"Analisando repositório {repo_index}/{len(repos)}: {repo_name}")
            
            page = 1
            pr_count = 0
            while pr_count < PRS_LIMIT:
                pr_url = f"{BASE_URL}/repos/{repo_name}/pulls?state=closed&per_page=100&page={page}"
                try:
                    response = fazer_requisicao_com_retry(pr_url, HEADERS)
                    prs = response.json()
                    
                    if not prs:
                        print(f"Não há mais PRs para analisar em {repo_name}")
                        break
                    
                    for pr in prs:
                        print(f"Analisando PR #{pr['number']} de {repo_name}")
                        pr_data = coletar_dados_pr(repo_name, pr)
                        if pr_data:
                            writer.writerow(pr_data)
                            pr_count += 1
                            if pr_count % 10 == 0:
                                print(f"  Processados {pr_count} PRs de {repo_name}")
                        
                        if pr_count >= PRS_LIMIT:
                            break
                    
                    page += 1
                    
                    # Respeitar o limite de taxa da API do GitHub
                    remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                    if remaining < 10:
                        reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                        sleep_time = max(reset_time - time.time(), 0) + 1
                        print(f"Limite de taxa próximo. Aguardando {sleep_time:.2f} segundos.")
                        time.sleep(sleep_time)
                    else:
                        time.sleep(1)
                
                except Exception as e:
                    print(f"Erro ao buscar PRs do repositório {repo_name}: {e}")
                    break
            
            print(f"  Total de PRs processados para {repo_name}: {pr_count}")

    print(f"Análise concluída. Os dados foram salvos em {CSV_FILE}")

if __name__ == "__main__":
    main()