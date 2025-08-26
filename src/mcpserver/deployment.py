from mcp.server.fastmcp import FastMCP


# implementation of server
mcp =FastMCP("Demo")


# add additional tool

@mcp.tool()
def add_numbers(a: int, b: int) -> int:
    """
    Adds two numbers.
    """
    return a + b