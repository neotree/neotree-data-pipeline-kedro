import html


def _format_validation_log_html(log_content):
    formatted_lines = []
    for line in str(log_content).splitlines():
        escaped_line = html.escape(line)
        if line.startswith("SCRIPT_HEADER: "):
            content = html.escape(line[len("SCRIPT_HEADER: "):])
            formatted_lines.append(
                f'<div class="script-header">{content}</div>'
            )
        elif line.startswith("SCRIPT_END: "):
            content = html.escape(line[len("SCRIPT_END: "):])
            formatted_lines.append(
                f'<div class="script-end">&gt;&gt;&gt;&gt;&gt;&gt; {content}</div>'
            )
        elif line.startswith(("ERROR:", "WARNING:", "WARN:")):
            formatted_lines.append(
                f'<div class="issue-line">{escaped_line}</div>'
            )
        else:
            formatted_lines.append(f"<div>{escaped_line or '&nbsp;'}</div>")
    return "\n".join(formatted_lines)


def get_html_validation_template(country, log_content):
    formatted_log_content = _format_validation_log_html(log_content)
    return f'''
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8">
        <title>Data Validator Email Template</title>
        <style>
          .container {{
            width: 100%;
            height: 100%;
            padding: 20px;
            background-color: #f4f4f4;
            font-family: Arial, sans-serif;
          }}
          .email {{
            width: 100%;
            margin: 0 auto;
            background-color: #fff;
            border: 1px solid #ccc;
            border-radius: 8px;
            overflow: hidden;
            white-space: pre-wrap;
            word-break: break-word;
            overflow-wrap: break-word;
          }}

          .email-header {{
            background-color: #1a313d;
            color: #fff;
            padding: 20px;
            text-align: center;
          }}
          .email-body {{
            font-family: Arial, sans-serif;
            font-size: 12px;
            margin: 0;
            padding: 0;
            color: #333;
          }}
          .email-footer {{
            background-color: #1a313d;
            color: #fff;
            padding: 15px;
            text-align: center;
            font-size: 0.85em;
          }}
          .validation-log {{
            background-color: #f8f8f8;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 0.9em;
            line-height: 1.35;
          }}
          .script-header {{
            color: #135f9c;
            font-weight: bold;
            font-size: 1.08em;
            border-bottom: 2px solid #135f9c;
            margin: 12px 0 8px;
            padding: 5px 0;
          }}
          .script-end {{
            color: #16823b;
            font-weight: bold;
            border-top: 1px solid #8bc99e;
            margin: 8px 0 14px;
            padding-top: 5px;
          }}
          .issue-line {{
            font-weight: bold;
            color: #9b1c1c;
          }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="email">
            <div class="email-header">
              <h1>DATA VALIDATION EXCEPTION</h1>
            </div>
            <div class="email-body">
              <p><strong>COUNTRY:</strong> {country}</p>
              <p><strong>DATA PIPELINE LOGS OUTPUT:</strong></p>
              <div class="validation-log">{formatted_log_content}</div>
            </div>
            <div class="email-footer">
               <p>© 2021 Neotree - All Rights Reserved. Charity no. 1186748, Registered office address: The Broadgate Tower, Third Floor, 20 Primrose Street, London EC2A 2RS | Designed by Morris Baradza</p>
            </div>
          </div>
        </div>
      </body>
    </html>
    '''
