import json
import pickle

def load_token_list(file_path):
    token_map = {}
    with open(file_path, 'r') as f:
        for line in f:
            if ':' in line:
                symbol, address = line.strip().split(':')
                token_map[address.strip()] = symbol.strip()
    return token_map

def save_as_json(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def save_as_bin(data, path):
    with open(path, 'wb') as f:
        pickle.dump(data, f)

if __name__ == "__main__":
    token_data = load_token_list('clean_tokens.txt')
    save_as_json(token_data, 'token_list.json')
    save_as_bin(token_data, 'token_list.bin')
    print(f"Сохранено {len(token_data)} токенов в 'token_list.json' и 'token_list.bin'")
