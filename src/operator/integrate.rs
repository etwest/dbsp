use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero},
    circuit::{Circuit, NodeId, OwnershipPreference, Stream},
    circuit_cache_key,
    operator::{
        z1::{DelayedFeedback, DelayedNestedFeedback},
        BinaryOperatorAdapter, Plus,
    },
    SharedRef,
};
use std::{ops::Add, rc::Rc};

circuit_cache_key!(IntegralId<C, D>(NodeId => Stream<C, D>));
circuit_cache_key!(NestedIntegralId<C, D>(NodeId => Stream<C, Rc<D>>));

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
    D: Add<Output = D> + AddByRef + AddAssignByRef + Clone + HasZero + 'static,
{
    /// Integrate the input stream.
    ///
    /// Computes the sum of values in the input stream.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbsp::{
    /// #     circuit::Root,
    /// #     operator::Generator,
    /// # };
    /// let root = Root::build(move |circuit| {
    ///     // Generate a stream of 1's.
    ///     let stream = circuit.add_source(Generator::new(|| 1));
    ///     // Integrate the stream.
    ///     let integral = stream.integrate();
    /// #   let mut counter1 = 0;
    /// #   integral.inspect(move |n| { counter1 += 1; assert_eq!(*n, counter1) });
    /// #   let mut counter2 = 0;
    /// #   integral.delay().inspect(move |n| { assert_eq!(*n, counter2); counter2 += 1; });
    /// })
    /// .unwrap();
    ///
    /// # for _ in 0..5 {
    /// #     root.step().unwrap();
    /// # }
    /// ```
    ///
    /// The above example generates the following input/output mapping:
    ///
    /// ```text
    /// input:  1, 1, 1, 1, 1, ...
    /// output: 1, 2, 3, 4, 5, ...
    /// ```
    pub fn integrate(&self) -> Stream<Circuit<P>, D> {
        if let Some(integral) = self
            .circuit()
            .cache()
            .get(&IntegralId::new(self.local_node_id()))
        {
            return integral.clone();
        }

        // Integration circuit:
        // ```
        //              input
        //   ┌─────────────────►
        //   │
        //   │    ┌───┐ current
        // ──┴───►│   ├────────►
        //        │ + │
        //   ┌───►│   ├────┐
        //   │    └───┘    │
        //   │             │
        //   │    ┌───┐    │
        //   │    │   │    │
        //   └────┤z-1├────┘
        //        │   │
        //        └───┴────────►
        //              delayed
        //              export
        // ```
        let feedback = DelayedFeedback::new(self.circuit());
        let integral = self.circuit().add_binary_operator_with_preference(
            Plus::new(),
            feedback.stream(),
            self,
            OwnershipPreference::STRONGLY_PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        );
        feedback.connect(&integral);

        self.circuit()
            .cache()
            .insert(IntegralId::new(self.local_node_id()), integral.clone());
        integral
    }

    /// Integrate stream of streams.
    ///
    /// Computes the sum of nested streams, i.e., rather than integrating values
    /// in each nested stream, this function sums up entire input streams
    /// across all parent timestamps, where the sum of streams is defined as
    /// a stream of point-wise sums of their elements: `integral[i,j] =
    /// sum(input[k,j]), k<=i`, where `stream[i,j]` is the value of `stream`
    /// at time `[i,j]`, `i` is the parent timestamp, and `j` is the child
    /// timestamp.
    ///
    /// Yields the sum element-by-element as the input stream is fed to the
    /// integral.
    ///
    /// # Examples
    ///
    /// Input stream (one row per parent timestamps):
    ///
    /// ```text
    /// 1 2 3 4
    /// 1 1 1 1 1
    /// 2 2 2 0 0
    /// ```
    ///
    /// Integral:
    ///
    /// ```text
    /// 1 2 3 4
    /// 2 3 4 5 1
    /// 4 5 6 5 1
    /// ```
    pub fn integrate_nested(&self) -> Stream<Circuit<P>, Rc<D>>
    where
        D: SharedRef<Target = D>,
    {
        if let Some(integral) = self
            .circuit()
            .cache()
            .get(&NestedIntegralId::new(self.local_node_id()))
        {
            return integral.clone();
        }

        let feedback = DelayedNestedFeedback::new(self.circuit());
        let integral = self.circuit().add_binary_operator_with_preference(
            <BinaryOperatorAdapter<D, _>>::new(Plus::new()),
            feedback.stream(),
            self,
            OwnershipPreference::STRONGLY_PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        );
        feedback.connect(&integral);

        self.circuit().cache().insert(
            NestedIntegralId::new(self.local_node_id()),
            integral.clone(),
        );

        integral
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::{FiniteMap, MapBuilder, ZSetHashMap},
        circuit::{trace::TraceMonitor, Root},
        operator::{DelayedFeedback, Generator},
    };

    use std::sync::{Arc, Mutex};

    #[test]
    fn scalar_integrate() {
        let root = Root::build(move |circuit| {
            let source = circuit.add_source(Generator::new(|| 1));
            let mut counter = 0;
            source.integrate().inspect(move |n| {
                counter += 1;
                assert_eq!(*n, counter);
            });
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }
    }

    #[test]
    fn zset_integrate() {
        let root = Root::build(move |circuit| {
            let mut counter1 = 0;
            let mut s = ZSetHashMap::new();
            let source = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s.increment(&counter1, 1);
                counter1 += 1;
                res
            }));

            let integral = source.integrate();
            let mut counter2 = 0;
            integral.inspect(move |s| {
                for i in 0..counter2 {
                    assert_eq!(s.lookup(&i), (counter2 - i) as isize);
                }
                counter2 += 1;
                assert_eq!(s.lookup(&counter2), 0);
            });
            let mut counter3 = 0;
            integral.delay().inspect(move |s| {
                for i in 1..counter3 {
                    assert_eq!(s.lookup(&(i - 1)), (counter3 - i) as isize);
                }
                counter3 += 1;
                assert_eq!(s.lookup(&(counter3 - 1)), 0);
            });
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }
    }

    /// ```text
    ///            ┌───────────────────────────────────────────────────────────────────────────────────┐
    ///            │                                                                                   │
    ///            │                           3,2,1,0,0,0                     3,2,1,0,                │
    ///            │                           4,3,2,1,0,0                     7,5,3,1,0,              │
    ///            │                    ┌───┐  2,1,0,0,0,0                     9,6,3,1,0,              │
    ///  3,4,2,5   │                    │   │  5,4,3,2,1,0                     14,10,6,3,1,0           │ 6,16,19,34
    /// ───────────┼──►delta0──────────►│ + ├──────────┬─────►integrate_nested───────────────integrate─┼─────────────►
    ///            │          3,0,0,0,0 │   │          │                                               │
    ///            │          4,0,0,0,0 └───┘          ▼                                               │
    ///            │          2,0,0,0,0   ▲    ┌──┐   ┌───┐                                            │
    ///            │          5,0,0,0,0   └────┤-1│◄──|z-1|                                            │
    ///            │                           └──┘   └───┘                                            │
    ///            │                                                                                   │
    ///            └───────────────────────────────────────────────────────────────────────────────────┘
    /// ```
    #[test]
    fn scalar_integrate_nested() {
        let root = Root::build(move |circuit| {
            TraceMonitor::attach(
                Arc::new(Mutex::new(TraceMonitor::new_panic_on_error())),
                circuit,
                "monitor",
            );

            let mut input = vec![3, 4, 2, 5].into_iter();

            let mut expected_counters =
                vec![3, 2, 1, 0, 4, 3, 2, 1, 0, 2, 1, 0, 0, 0, 5, 4, 3, 2, 1, 0].into_iter();
            let mut expected_integrals =
                vec![3, 2, 1, 0, 7, 5, 3, 1, 0, 9, 6, 3, 1, 0, 14, 10, 6, 3, 1, 0].into_iter();
            let mut expected_outer_integrals = vec![6, 16, 19, 34].into_iter();

            let source = circuit.add_source(Generator::new(move || input.next().unwrap()));
            let integral = circuit
                .iterate_with_condition(|child| {
                    let source = source.delta0(&child);
                    let feedback = DelayedFeedback::new(child);
                    let plus =
                        source.plus(&feedback.stream().apply(|&n| if n > 0 { n - 1 } else { n }));
                    plus.inspect(move |n| assert_eq!(*n, expected_counters.next().unwrap()));
                    feedback.connect(&plus);
                    let integral = plus.integrate_nested();
                    integral.inspect(move |n| assert_eq!(**n, expected_integrals.next().unwrap()));
                    Ok((
                        integral.condition(|n| **n == 0),
                        integral.apply(|rc| **rc).integrate().export(),
                    ))
                })
                .unwrap();
            integral.inspect(move |n| assert_eq!(*n, expected_outer_integrals.next().unwrap()))
        })
        .unwrap();

        for _ in 0..4 {
            root.step().unwrap();
        }
    }
}