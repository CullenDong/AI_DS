"""Example: run the agent on a sample task."""
import os

from anthropic import Anthropic

from agent.loop import run_agent


if __name__ == "__main__":
    client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    run_agent(client, "Load data/samples/iris.csv as 'iris', describe it, and plot a histogram of sepal_length.")
