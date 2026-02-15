from prefect import flow

@flow
def test():
    print("Prefect test")

test()