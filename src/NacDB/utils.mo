import I "mo:base/Iter";
import Nac "../NacDB";

class SharedNacDBIter(iter: I.Iter<(Text, Nac.AttributeValue)>) {
    public shared func next(): async ?(Text, Nac.AttributeValue) {
        iter.next()
    }
}