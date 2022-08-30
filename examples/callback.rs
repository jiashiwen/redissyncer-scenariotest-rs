type Callback = fn(&str);

struct Processor {
    callback: Callback,
}

impl Processor {
    fn set_callback(&mut self, c: Callback) {
        self.callback = c;
    }

    fn process_events(&self, str: &str) {
        (self.callback)(str);
    }
}

fn simple_callback(str: &str) {
    println!("{:?}", str);
}

fn simple_callback2(_str: &str) {
    println!("2");
}

fn main() {
    let mut p = Processor {
        callback: simple_callback,
    };
    p.process_events("test"); // hello world!

    p.set_callback(simple_callback2);
    p.process_events("1");
}
