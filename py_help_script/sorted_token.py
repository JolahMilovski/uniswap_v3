import re

def extract_tokens(input_file, output_file):
    # Обновленный паттерн для названий токенов
    token_pattern = r'(?:^|\s|\/)((?:[A-Za-z][A-Za-z0-9]*(?:\.?[a-z]{1,2})?)|(?:[A-Za-z]+[A-Za-z0-9]*(?:\.[A-Za-z][A-Za-z0-9]*)?))(?:\s*[☕₮])?(?=\/|$|\s|\.|\t)'
    
    # Список технических слов для исключения
    exclude_words = {'logo', 'pool', 'filepath'}
    
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим все токены
    tokens = re.findall(token_pattern, content)
    
    # Фильтруем и сортируем токены
    filtered_tokens = sorted(set(
        token for token in tokens 
        if token and not token.isspace() 
        and token.lower() not in exclude_words
    ))
    
    # Записываем результат
    with open(output_file, 'w', encoding='utf-8') as f:
        for token in filtered_tokens:
            f.write(f"{token}\n")
    
    print(f"Найдено {len(filtered_tokens)} уникальных токенов")

# Использование
input_file = "all_dex_token.txt"
output_file = "clean_tokens.txt"
extract_tokens(input_file, output_file)