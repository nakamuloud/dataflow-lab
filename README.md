# What's this?
Apache beam based dev kit with vscode/devcontainer 
# Scenarios

1. 名前と住所のJSONが入力される
```json
	{
		"name": "Jared Welch",
		"address": "59109 Brown Rest Maureenbury, KS 24574"
	}
```
2. 名前から姓名を分けて追加(ParDo)
```json
	{
		"name": "Jared Welch",
		"address": "59109 Brown Rest Maureenbury, KS 24574",
		"firstname": "Jared",
		"lastname": "Welch"
	}
```
3. 各データをマージしてsetsとnamesを出力
```json
	[
		{
			"sets":
				{
					"name": "Jared Welch",
					"address": "59109 Brown Rest Maureenbury, KS 24574",
					"firstname": "Jared",
					"lastname": "Welch"
				},
				{
					"name": "Chantal Gomes",
					"address": "rue Roussel 32872 Peron-la-Forêt",
					"firstname": "Chantal",
					"lastname": "Gomes"
				}
		},
		{
			"names":["Jared Welch","Chantal Gomes"]
		}
	]
```