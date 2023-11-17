# https://stackoverflow.com/questions/59371631/send-automated-messages-to-microsoft-teams-using-python
# https://dev.to/shadow_b/how-to-send-message-to-teams-using-python-496g
# https://teams.microsoft.com/l/channel/19%3Aed85caf75bec4891a8bc0482d4a7ec8e%40thread.tacv2/4.%20Errors%20-%20Notification?groupId=964795fe-8449-4643-95b3-ae266a31cb44&tenantId=5675d321-19d1-4c95-9684-2c28ac8f80a4

import pymsteams

# Initialize the connector card with your webhook URL
myTeamsMessage = pymsteams.connectorcard("YOUR_WEBHOOK_URL_HERE")

# Set the message color
myTeamsMessage.color("#F8C471")

# Add your message text
myTeamsMessage.text("Hello, this is a sample message!")

# Send the message
myTeamsMessage.send()