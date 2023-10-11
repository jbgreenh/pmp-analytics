import toml

with open('../secrets_test.toml', 'r') as f:
    secrets = toml.load(f)

print(secrets['secret']['one'])
print(secrets['secret']['two'])
print('yes' if secrets['secret']['three'] else 'no')

secrets['secret 2'] = {}    # empty dict, errors without this line when adding new section, don't need when section already exitsts
secrets['secret 2']['four'] = 'secret 2 four update'   # dict entry

with open('secrets_test.toml', 'w') as f:
    toml.dump(secrets, f)