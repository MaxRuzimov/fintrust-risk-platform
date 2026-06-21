import requests

def decode_secret_message(url):
    # Convert published URL to plain text export
    export_url = url.replace('/pub', '/export?format=txt')
    response = requests.get(export_url)
    lines = response.text.strip().split('\n')
    
    grid = {}
    max_x = 0
    max_y = 0
    
    for line in lines:
        parts = line.strip().split()
        if len(parts) == 3:
            try:
                char = parts[0]
                x = int(parts[1])
                y = int(parts[2])
                grid[(x, y)] = char
                max_x = max(max_x, x)
                max_y = max(max_y, y)
            except ValueError:
                continue
    
    for y in range(max_y + 1):
        row = ''
        for x in range(max_x + 1):
            row += grid.get((x, y), ' ')
        print(row)

decode_secret_message("https://docs.google.com/document/d/e/2PACX-1vSvM5gDlNvt7npYHhp_XfsJvuntUhq184By5xO_pA4b_gCWeXb6dM6ZxwN8rE6S4ghUsCj2VKR21oEP/pub")