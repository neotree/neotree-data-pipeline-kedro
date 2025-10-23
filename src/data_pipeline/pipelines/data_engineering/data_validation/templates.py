def get_html_validation_template(country, log_content):
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
          pre {{
            background-color: #f8f8f8;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            overflow-x: auto;
            font-size: 0.9em;
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
              <pre>{log_content}</pre>
            </div>
            <div class="email-footer">
               <p>© 2021 Neotree - All Rights Reserved. Charity no. 1186748, Registered office address: The Broadgate Tower, Third Floor, 20 Primrose Street, London EC2A 2RS | Designed by Morris Baradza</p>
            </div>
          </div>
        </div>
      </body>
    </html>
    '''
